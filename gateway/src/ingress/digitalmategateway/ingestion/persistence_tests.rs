//! Actual PostgreSQL integration; explicit test-owned database only.
use super::super::composio::WebhookVerifier;
use super::tests::{binding, body, headers, NOW, SECRET};
use super::*;
use deadpool_postgres::{Manager, Pool};
use std::time::Duration;

pub(super) async fn store() -> Inbox {
    let dsn = std::env::var("ADX_INGESTION_TEST_DATABASE_URL")
        .expect("explicit isolated PostgreSQL test URL required");
    let config: tokio_postgres::Config = dsn.parse().unwrap();
    assert!(
        config
            .get_dbname()
            .unwrap_or("")
            .starts_with("dm_ingestion_test_"),
        "refuse a non-test database"
    );
    let pool = Pool::builder(Manager::new(config, tokio_postgres::NoTls))
        .max_size(6)
        .build()
        .unwrap();
    let client = pool.get().await.unwrap();
    client
        .batch_execute("DROP SCHEMA IF EXISTS digitalmate_ingestion CASCADE")
        .await
        .unwrap();
    drop(client);
    Inbox::initialize(
        pool,
        InboxLimits {
            max_pending_events: 100,
            max_pending_bytes: 8_000_000,
            per_subscription_events: 80,
            debounce: Duration::ZERO,
            dedupe_retention: Duration::from_secs(7 * 86_400),
        },
    )
    .await
    .unwrap()
}

async fn receive(store: &Inbox, id: &str) -> ReceiveReceipt {
    let raw = body(id, "private mail");
    let proof = WebhookVerifier::new(&[SECRET])
        .unwrap()
        .verify(&headers(&raw, NOW, SECRET), &raw, NOW)
        .unwrap();
    store.receive(proof).await.unwrap()
}

// One test owns one schema; scenarios below intentionally share/reopen the same
// real database to exercise commit/restart and multi-worker fences.
#[tokio::test]
#[ignore = "requires an explicit task-owned PostgreSQL database"]
async fn durable_receive_restart_multi_worker_and_lost_ack_contract() {
    let store = store().await;
    let current = binding();
    store.register(&current).await.unwrap();
    assert!(!receive(&store, "msg_A").await.duplicate);
    assert!(receive(&store, "msg_A").await.duplicate);
    receive(&store, "msg_B").await;
    let reopened = Inbox::from_pool(store.pool().clone(), store.limits().clone())
        .await
        .unwrap();
    let (one, two) = tokio::join!(
        reopened.claim(Duration::from_secs(30)),
        store.claim(Duration::from_secs(30))
    );
    let leases: Vec<_> = [one.unwrap(), two.unwrap()].into_iter().flatten().collect();
    assert_eq!(leases.len(), 1);
    let first = &leases[0];
    assert_eq!(first.batch.events.len(), 2);
    receive(&store, "msg_C").await; // Must survive ACK of the fixed A/B set.
    let ack = DurableAck {
        version: 1,
        batch_id: first.batch.batch_id.clone(),
        digest: first.batch.digest.clone(),
        state: "accepted".into(),
    };
    store
        .retry(first, Duration::ZERO, "delivery_unknown")
        .await
        .unwrap();
    let retry = reopened
        .claim(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retry.batch.batch_id, first.batch.batch_id);
    assert_eq!(
        store.accept(first, &ack).await.err(),
        Some(Error::LeaseLost)
    );
    reopened.accept(&retry, &ack).await.unwrap();
    let next = store.claim(Duration::from_secs(30)).await.unwrap().unwrap();
    assert_eq!(next.batch.events.len(), 1);
    assert_eq!(next.batch.events[0].source_event_id, "msg_C");
    let mut revoked = current.clone();
    revoked.subscription_generation += 1;
    revoked.enabled = false;
    store.register(&revoked).await.unwrap();
    assert_eq!(
        store
            .accept(
                &next,
                &DurableAck {
                    version: 1,
                    batch_id: next.batch.batch_id.clone(),
                    digest: next.batch.digest.clone(),
                    state: "accepted".into()
                }
            )
            .await
            .err(),
        Some(Error::LeaseLost)
    );
    assert!(store
        .claim(Duration::from_secs(30))
        .await
        .unwrap()
        .is_none());
    assert!(store.register(&current).await.is_err());

    // Explicit bounded capacity is transactional; exact duplicates remain
    // retryable even when full and a conflicting replay never changes a row.
    let mut restored = revoked.clone();
    restored.subscription_generation += 1;
    restored.enabled = true;
    store.register(&restored).await.unwrap();
    let limited = Inbox::from_pool(
        store.pool().clone(),
        InboxLimits {
            max_pending_events: 2,
            max_pending_bytes: 1_048_576,
            per_subscription_events: 2,
            debounce: Duration::ZERO,
            dedupe_retention: Duration::from_secs(7 * 86_400),
        },
    )
    .await
    .unwrap();
    receive(&limited, "msg_D").await;
    receive(&limited, "msg_E").await;
    assert!(receive(&limited, "msg_D").await.duplicate);
    let raw = body("msg_F", "private mail");
    let proof = WebhookVerifier::new(&[SECRET])
        .unwrap()
        .verify(&headers(&raw, NOW, SECRET), &raw, NOW)
        .unwrap();
    assert_eq!(limited.receive(proof).await.err(), Some(Error::Capacity));
    let raw = body("msg_D", "changed contents");
    let proof = WebhookVerifier::new(&[SECRET])
        .unwrap()
        .verify(&headers(&raw, NOW, SECRET), &raw, NOW)
        .unwrap();
    assert_eq!(
        limited.receive(proof).await.err(),
        Some(Error::ConflictingInput)
    );
    let expired = limited
        .claim(Duration::from_millis(100))
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let resumed = limited
        .claim(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(expired.batch.batch_id, resumed.batch.batch_id);
    assert_eq!(
        limited.renew(&expired, Duration::from_secs(30)).await.err(),
        Some(Error::LeaseLost)
    );
    let ack = DurableAck {
        version: 1,
        batch_id: resumed.batch.batch_id.clone(),
        digest: resumed.batch.digest.clone(),
        state: "accepted".into(),
    };
    limited.accept(&resumed, &ack).await.unwrap();

    // Force PostgreSQL to reject at COMMIT, not at INSERT. No durable 2xx/receipt
    // may escape, and a retry after the fault is removed inserts exactly once.
    let client = store.pool().get().await.unwrap();
    client.batch_execute("CREATE FUNCTION digitalmate_ingestion.reject_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'controlled commit rejection'; END $$; CREATE CONSTRAINT TRIGGER reject_event_commit AFTER INSERT ON digitalmate_ingestion.events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION digitalmate_ingestion.reject_commit();").await.unwrap();
    let raw = body("msg_commit", "private mail");
    let proof = WebhookVerifier::new(&[SECRET])
        .unwrap()
        .verify(&headers(&raw, NOW, SECRET), &raw, NOW)
        .unwrap();
    assert_eq!(store.receive(proof).await.err(), Some(Error::Unavailable));
    let count: i64 = client
        .query_one(
            "SELECT count(*)::bigint FROM digitalmate_ingestion.events WHERE event_id='msg_commit'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
    client.batch_execute("DROP TRIGGER reject_event_commit ON digitalmate_ingestion.events; DROP FUNCTION digitalmate_ingestion.reject_commit()").await.unwrap();
    drop(client);
    assert!(!receive(&store, "msg_commit").await.duplicate);
    assert!(receive(&store, "msg_commit").await.duplicate);
    let client = store.pool().get().await.unwrap();
    client.batch_execute("UPDATE digitalmate_ingestion.events SET finished_at=clock_timestamp()-interval '8 days' WHERE state='delivered'; UPDATE digitalmate_ingestion.batches SET finished_at=clock_timestamp()-interval '8 days' WHERE state='accepted'; UPDATE digitalmate_ingestion.events SET received_at=clock_timestamp()-interval '8 days' WHERE state='pending'").await.unwrap();
    drop(client);
    assert!(store.purge_terminal(1000).await.unwrap() >= 4);
    let client = store.pool().get().await.unwrap();
    let pending:i64=client.query_one("SELECT count(*)::bigint FROM digitalmate_ingestion.events WHERE event_id='msg_commit' AND state='pending'",&[]).await.unwrap().get(0);
    assert_eq!(pending, 1, "retention must never delete a pending input");
}
