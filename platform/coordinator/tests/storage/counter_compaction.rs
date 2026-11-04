use super::*;
use adx_coordinator::storage::{BusinessCounters, RedisStore};

// These are state/convergence tests, with synchronous AOF enabled. Allow fsync
// on a shared CI filesystem without turning its latency into a storage failure.
async fn counter_store(rig: &common::Redis) -> RedisStore {
    RedisStore::connect(&rig.url, "test", std::time::Duration::from_secs(5))
        .await
        .unwrap()
}

async fn business_fields(rig: &common::Redis) -> Vec<String> {
    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("HKEYS")
        .arg("adx:{test}:control:v1:business")
        .query_async(&mut connection)
        .await
        .unwrap()
}

async fn create(
    session: &adx_coordinator::storage::Session,
    id: &str,
    generation: u64,
) -> EnvironmentRecord {
    let assignment = Assignment {
        environment_id: id.into(),
        node_id: "node".into(),
        shard_id: 0,
        generation,
        devices: vec![],
    };
    session.reserve(spec(id), assignment.clone()).await.unwrap();
    let record = running(spec(id), assignment);
    session.commit(record.clone()).await.unwrap();
    record
}

fn deletion(mut record: EnvironmentRecord) -> EnvironmentRecord {
    record.state = EnvironmentState::Deleted;
    record.resources_held = false;
    record.runtime.ip = None;
    record.revision += 1;
    record
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn retired_counter_storage_is_bounded_by_live_objects_and_fixed_buckets() {
    let rig = common::Redis::new().await;
    let session = counter_store(&rig).await.begin(1).await.unwrap();
    register(&session, "node").await;
    for index in 0..512 {
        let record = create(&session, &format!("churn-{index}"), index + 1).await;
        session.commit(deletion(record)).await.unwrap();
    }
    assert!(session.snapshot().await.unwrap().environments.is_empty());
    assert_eq!(
        session.business_counters().await.unwrap().creations,
        [512, 0, 0]
    );
    let fields = business_fields(&rig).await;
    assert!(
        fields.len() <= 64,
        "historical counters grew to {} fields",
        fields.len()
    );
    assert!(fields.iter().all(|field| field.starts_with("retired:")));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn concurrent_retirement_and_delete_replay_preserve_counts_across_redis_restart() {
    let mut rig = common::Redis::new().await;
    let session = counter_store(&rig).await.begin(1).await.unwrap();
    register(&session, "node").await;
    let mut records = Vec::new();
    for index in 0..96 {
        records.push(create(&session, &format!("parallel-{index}"), index + 1).await);
    }
    for chunk in records.chunks(16) {
        let jobs = chunk.iter().map(|record| {
            let session = session.clone();
            let deleted = deletion(record.clone());
            async move {
                session.commit(deleted.clone()).await.unwrap();
                session.commit(deleted).await.unwrap();
            }
        });
        futures_util::future::join_all(jobs).await;
    }
    let expected = session.business_counters().await.unwrap();
    assert_eq!(expected.creations, [96, 0, 0]);
    assert!(business_fields(&rig).await.len() <= 64);
    rig.crash();
    rig.start().await;
    let restarted = counter_store(&rig).await.begin(1).await.unwrap();
    assert_eq!(restarted.business_counters().await.unwrap(), expected);
    assert!(restarted.snapshot().await.unwrap().environments.is_empty());
    assert!(business_fields(&rig).await.len() <= 64);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn recreated_name_counts_each_generation_once_and_fences_old_deletion() {
    let rig = common::Redis::new().await;
    let session = counter_store(&rig).await.begin(1).await.unwrap();
    register(&session, "node").await;
    let mut previous: Option<EnvironmentRecord> = None;
    for generation in 1..=32 {
        let record = create(&session, "reused", generation).await;
        if let Some(old) = previous.as_ref() {
            assert!(session.commit(old.clone()).await.is_err());
        }
        let deleted = deletion(record);
        session.commit(deleted.clone()).await.unwrap();
        session.commit(deleted.clone()).await.unwrap();
        previous = Some(deleted);
        assert_eq!(
            session.business_counters().await.unwrap().creations,
            [generation, 0, 0]
        );
        assert!(business_fields(&rig)
            .await
            .iter()
            .all(|field| field.starts_with("retired:")));
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn startup_compacts_orphaned_history_without_losing_live_or_node_facts() {
    let rig = common::Redis::new().await;
    let store = counter_store(&rig).await;
    let session = store.begin(1).await.unwrap();
    register(&session, "node").await;
    create(&session, "live", 1).await;
    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    for index in 0..192 {
        let (field, value) = if index % 2 == 0 {
            (
                format!("environment:old-{index}"),
                BusinessCounters {
                    creations: [1, 2, 3],
                    ..Default::default()
                },
            )
        } else {
            (
                format!("node:old-{index}"),
                BusinessCounters {
                    worker_incidents: 1,
                    ..Default::default()
                },
            )
        };
        redis::cmd("HSET")
            .arg("adx:{test}:control:v1:business")
            .arg(field)
            .arg(serde_json::to_string(&value).unwrap())
            .query_async::<()>(&mut connection)
            .await
            .unwrap();
    }
    let expected = session.business_counters().await.unwrap();
    let restarted = store.begin(1).await.unwrap();
    assert_eq!(restarted.business_counters().await.unwrap(), expected);
    assert_eq!(expected.creations, [97, 192, 288]);
    assert_eq!(expected.worker_incidents, 96);
    assert_eq!(
        restarted.get("live").await.unwrap().result.unwrap().state,
        EnvironmentState::Running,
        "startup compaction must retain the live lifecycle record"
    );
    let fields = business_fields(&rig).await;
    assert!(fields.len() <= 65);
    assert_eq!(
        fields
            .iter()
            .filter(|field| field.starts_with("environment:"))
            .count(),
        1
    );
    assert!(fields
        .iter()
        .all(|field| field == "environment:live" || field.starts_with("retired:")));
    let again = store.begin(1).await.unwrap();
    assert_eq!(again.business_counters().await.unwrap(), expected);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn node_incident_replay_uses_bounded_retired_buckets() {
    let rig = common::Redis::new().await;
    let store = counter_store(&rig).await;
    let session = store.begin(1).await.unwrap();
    for index in 0..96 {
        let id = format!("node-{index}");
        session
            .register_session(
                node(&id),
                "node:9000".into(),
                "node:9001".into(),
                Some(adx_coordinator::storage::NodeSession {
                    id: "boot".into(),
                    sequence: 1,
                    routable: true,
                }),
            )
            .await
            .unwrap();
        session.invalidate_node(&id, "boot").await.unwrap();
        session.invalidate_node(&id, "boot").await.unwrap();
    }
    assert_eq!(
        session.business_counters().await.unwrap().worker_incidents,
        96
    );
    assert!(business_fields(&rig).await.len() <= 64);
    assert!(business_fields(&rig)
        .await
        .iter()
        .all(|field| field.starts_with("retired:")));
    assert_eq!(
        store
            .begin(1)
            .await
            .unwrap()
            .business_counters()
            .await
            .unwrap()
            .worker_incidents,
        96
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn retirement_keeps_u64_precision_and_overflow_leaves_lifecycle_unchanged() {
    let rig = common::Redis::new().await;
    let session = counter_store(&rig).await.begin(1).await.unwrap();
    register(&session, "node").await;
    let first = create(&session, "large-count", 1).await;
    session.commit(deletion(first)).await.unwrap();
    let fields = business_fields(&rig).await;
    assert_eq!(fields.len(), 1);
    let mut connection = redis::Client::open(rig.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let seeded = BusinessCounters {
        creations: [u64::MAX - 1, 0, 0],
        ..Default::default()
    };
    redis::cmd("HSET")
        .arg("adx:{test}:control:v1:business")
        .arg(&fields[0])
        .arg(serde_json::to_string(&seeded).unwrap())
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    let second = create(&session, "large-count", 2).await;
    session.commit(deletion(second)).await.unwrap();
    assert_eq!(
        session.business_counters().await.unwrap().creations,
        [u64::MAX, 0, 0]
    );
    let third = create(&session, "large-count", 3).await;
    assert_eq!(
        session.commit(deletion(third.clone())).await,
        Err(Error::Conflict)
    );
    assert_eq!(
        session.get("large-count").await.unwrap().result,
        Some(third)
    );
    let raw: String = redis::cmd("HGET")
        .arg("adx:{test}:control:v1:business")
        .arg(&fields[0])
        .query_async(&mut connection)
        .await
        .unwrap();
    let retained: BusinessCounters = serde_json::from_str(&raw).unwrap();
    assert_eq!(retained.creations, [u64::MAX, 0, 0]);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn corrupt_history_does_not_block_startup_or_discard_repairable_facts() {
    let rig = common::Redis::new().await;
    let store = counter_store(&rig).await;
    let session = store.begin(1).await.unwrap();
    register(&session, "node").await;
    create(&session, "live", 1).await;
    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let historical = BusinessCounters {
        creations: [2, 0, 0],
        ..Default::default()
    };
    redis::cmd("HSET")
        .arg("adx:{test}:control:v1:business")
        .arg("environment:corrupt")
        .arg("invalid historical record")
        .arg("environment:healthy")
        .arg(serde_json::to_string(&historical).unwrap())
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    let recovered = store.begin(1).await.unwrap();
    assert_eq!(
        recovered.get("live").await.unwrap().result.unwrap().state,
        EnvironmentState::Running
    );
    assert!(recovered.business_counters().await.is_err());
    let fields = business_fields(&rig).await;
    assert!(fields.iter().any(|field| field == "environment:corrupt"));
    assert!(!fields.iter().any(|field| field == "environment:healthy"));
    let rpc = adx_coordinator::rpc::CoordinatorRpc::new(
        recovered.clone(),
        Placement::Pack,
        adx_protocol::auth::Peers::network(),
        adx_transport::rpc::RpcClient::network(adx_protocol::auth::Principal::Coordinator),
        std::time::Duration::from_secs(2),
    )
    .await
    .expect("observation corruption must not block RPC service startup");
    assert!(matches!(rpc.metrics().await, Err(Error::Unavailable(_))));
    // A bad observability field must not prevent new lifecycle commits.
    create(&recovered, "after-recovery", 2).await;
    redis::cmd("HSET")
        .arg("adx:{test}:control:v1:business")
        .arg("environment:corrupt")
        .arg(serde_json::to_string(&historical).unwrap())
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    let mut refreshed = None;
    for _ in 0..40 {
        if let Ok(metrics) = rpc.metrics().await {
            refreshed = Some(metrics);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let metrics = refreshed.expect("statistics did not refresh after history repair");
    assert!(metrics.contains("adx_environment_owned_creation_results_total{outcome=\"success\"} 6"));
    let repaired = store.begin(1).await.unwrap();
    assert_eq!(
        repaired.business_counters().await.unwrap().creations,
        [6, 0, 0]
    );
    assert!(!business_fields(&rig)
        .await
        .iter()
        .any(|field| field == "environment:corrupt"));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn configured_operation_budget_outlives_driver_default_and_remains_bounded() {
    let rig = common::Redis::new().await;
    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let control_config = redis::AsyncConnectionConfig::new()
        .set_response_timeout(Some(std::time::Duration::from_secs(3)));
    let mut control = client
        .get_multiplexed_async_connection_with_config(&control_config)
        .await
        .unwrap();
    let long = RedisStore::connect(&rig.url, "test", std::time::Duration::from_secs(3))
        .await
        .unwrap();
    let session = long.begin(1).await.unwrap();
    register(&session, "node").await;
    // Keep the short budget below the pause while allowing shared CI AOF latency.
    let short = RedisStore::connect(&rig.url, "test", std::time::Duration::from_millis(500))
        .await
        .unwrap();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(800)
        .arg("ALL")
        .query_async::<()>(&mut control)
        .await
        .unwrap();
    let started = std::time::Instant::now();
    assert!(
        session.snapshot().await.is_ok(),
        "configured budget was truncated by the driver's default"
    );
    assert!(started.elapsed() >= std::time::Duration::from_millis(600));
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(1500)
        .arg("ALL")
        .query_async::<()>(&mut control)
        .await
        .unwrap();
    let started = std::time::Instant::now();
    assert!(matches!(short.begin(1).await, Err(Error::Unavailable(_))));
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
    // Wait for Redis to resume before testing replacement of the timed-out connection.
    let pong = redis::cmd("PING")
        .query_async::<String>(&mut control)
        .await
        .expect("Redis did not resume after CLIENT PAUSE");
    assert_eq!(pong, "PONG");
    short
        .begin(1)
        .await
        .expect("Redis connection did not recover");
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn rpc_startup_keeps_epoch_fencing_fatal() {
    let rig = common::Redis::new().await;
    let store = counter_store(&rig).await;
    let stale = store.begin(1).await.unwrap();
    store.begin(1).await.unwrap();
    let result = adx_coordinator::rpc::CoordinatorRpc::new(
        stale,
        Placement::Pack,
        adx_protocol::auth::Peers::network(),
        adx_transport::rpc::RpcClient::network(adx_protocol::auth::Principal::Coordinator),
        std::time::Duration::from_secs(2),
    )
    .await;
    assert!(matches!(result, Err(Error::Conflict)));
}
