use std::sync::Arc;

use afs_error::{CONFIG_INVALID, IO_INVALID};

use afs::meta::store::{
    MetaEntity, MetaRead, MetaStore, NodeSessionLease, RequestKey, Store, TxnOutcome,
    redis::RedisBackend,
};

const SNAPSHOT_KEY: &str = "afs:meta:snapshot";

fn lease(session_id: &str) -> NodeSessionLease {
    NodeSessionLease {
        node_id: "node-a".into(),
        session_id: session_id.into(),
        grpc_addr: "http://node-a:7400".into(),
        data_addr: "http://node-a:7500".into(),
        rest_addr: "http://node-a:7600".into(),
        storage_devices: Vec::new(),
        lease_ttl: std::time::Duration::from_secs(30),
    }
}

fn dedicated_endpoint() -> String {
    std::env::var("AFS_TEST_REDIS_STORE_BUSINESS_ENDPOINT")
        .or_else(|_| std::env::var("AFS_TEST_REDIS_STORE_ENDPOINT"))
        .expect("a dedicated Redis endpoint is required; no implicit backend PASS")
}

async fn redis_connection(endpoint: &str) -> redis::aio::MultiplexedConnection {
    redis::Client::open(endpoint)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

async fn reset_dedicated_database(endpoint: &str) {
    let mut connection = redis_connection(endpoint).await;
    let _: String = redis::cmd("FLUSHDB")
        .query_async(&mut connection)
        .await
        .unwrap();
}

/// Run only against a fresh dedicated Redis database configured with AOF always
/// and noeviction. The test writes the fixed production snapshot key and leaves
/// it for replay inspection.
#[tokio::test]
#[ignore = "requires a dedicated durable Redis; see docs/testing/validation.md"]
async fn store_replays_business_state_from_dedicated_redis() {
    let endpoint = dedicated_endpoint();
    reset_dedicated_database(&endpoint).await;

    let backend = Arc::new(RedisBackend::connect(endpoint.clone()).await.unwrap());
    let store = Store::open(backend).await.unwrap();
    store
        .register_node_session(
            RequestKey::new("node-a", "redis-r1"),
            lease("redis-session"),
        )
        .await
        .unwrap();
    drop(store);

    let reopened = Store::open(Arc::new(RedisBackend::connect(endpoint).await.unwrap()))
        .await
        .unwrap();
    let current = reopened
        .read(MetaRead::CurrentNodeSession {
            node_id: "node-a".into(),
        })
        .await
        .unwrap();
    assert!(
        matches!(current.entity, Some(MetaEntity::NodeSession(session)) if session.session_id == "redis-session")
    );
    let replay = reopened
        .register_node_session(
            RequestKey::new("node-a", "redis-r1"),
            lease("redis-session"),
        )
        .await
        .unwrap();
    assert!(matches!(
        replay,
        TxnOutcome::ConditionFailed {
            existing_outcome: Some(_),
            ..
        }
    ));
}

/// Reads the snapshot left by `store_replays_business_state_from_dedicated_redis`.
/// Use it after an external Redis process restart to prove AOF-backed recovery.
#[tokio::test]
#[ignore = "requires an existing business snapshot in a restarted dedicated Redis"]
async fn loads_existing_business_state_after_redis_restart() {
    let endpoint = dedicated_endpoint();
    let reopened = Store::open(Arc::new(RedisBackend::connect(endpoint).await.unwrap()))
        .await
        .unwrap();
    let current = reopened
        .read(MetaRead::CurrentNodeSession {
            node_id: "node-a".into(),
        })
        .await
        .unwrap();
    assert!(
        matches!(current.entity, Some(MetaEntity::NodeSession(session)) if session.session_id == "redis-session")
    );
}

/// A Redis key expiration would silently remove Meta authority, so startup/load
/// must reject a snapshot key with TTL instead of treating it as normal data.
#[tokio::test]
#[ignore = "requires a dedicated durable Redis; see docs/testing/validation.md"]
async fn rejects_snapshot_key_with_ttl() {
    let endpoint = dedicated_endpoint();
    reset_dedicated_database(&endpoint).await;

    let mut connection = redis_connection(&endpoint).await;
    let _: usize = redis::cmd("HSET")
        .arg(SNAPSHOT_KEY)
        .arg("version")
        .arg("1")
        .arg("payload")
        .arg(b"{}".as_slice())
        .query_async(&mut connection)
        .await
        .unwrap();
    let _: bool = redis::cmd("EXPIRE")
        .arg(SNAPSHOT_KEY)
        .arg(60)
        .query_async(&mut connection)
        .await
        .unwrap();

    let backend = RedisBackend::connect(endpoint).await.unwrap();
    let error = backend.load().await.unwrap_err();
    assert_eq!(error.code(), IO_INVALID);
    assert!(error.to_string().contains("expiration"));
}

/// The Redis Meta backend is a durable store, not a cache. An instance without
/// the required AOF/noeviction contract must be rejected before serving Meta.
#[tokio::test]
#[ignore = "requires a deliberately unsafe Redis endpoint"]
async fn rejects_unsafe_redis_config() {
    let endpoint = std::env::var("AFS_TEST_REDIS_UNSAFE_ENDPOINT")
        .expect("AFS_TEST_REDIS_UNSAFE_ENDPOINT is required");
    let error = match RedisBackend::connect(endpoint).await {
        Ok(_) => panic!("unsafe Redis config was accepted"),
        Err(error) => error,
    };
    assert_eq!(error.code(), CONFIG_INVALID);
    assert!(error.to_string().contains("redis meta store requires"));
}
