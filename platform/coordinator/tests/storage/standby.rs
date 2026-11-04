//! C-2/C-3: standby read-only synchronization and lease-gated promotion.
//! The standby never touches the control header, marks its snapshot stale on
//! refresh failures, and competes for the lease only once it is free.
use super::common::Redis;
use adx_coordinator::lease::CoordinatorLease;
use adx_coordinator::standby::Standby;
use std::time::Duration;

#[tokio::test]
async fn standby_reads_never_touch_the_control_header_and_track_the_primary() {
    let redis = Redis::new().await;
    let store = redis.store().await;
    let first = store.begin(1).await.unwrap();
    let before = store.read_snapshot().await.unwrap();

    let standby = Standby::new(store.clone());
    standby.refresh().await;
    let state = standby.state().await.unwrap();
    assert_eq!(state.revision, before.revision);
    assert_eq!(state.generation, before.generation);
    assert!(!state.stale);

    // A new primary advances the epoch and revision; the standby follows
    // read-only — the header it observes moves, the one it writes never
    // does (it writes nothing).
    let second = store.begin(1).await.unwrap();
    assert!(second.epoch() > first.epoch());
    standby.refresh().await;
    let after = store.read_snapshot().await.unwrap();
    let state = standby.state().await.unwrap();
    assert_eq!(state.revision, after.revision);
    assert!(state.revision > before.revision);
    assert!(!state.stale);
}

#[tokio::test]
async fn standby_marks_its_snapshot_stale_and_recovers() {
    let mut redis = Redis::new().await;
    let store = redis.store().await;
    store.begin(1).await.unwrap();
    let standby = Standby::new(store.clone());
    standby.refresh().await;
    assert!(!standby.state().await.unwrap().stale);

    redis.crash();
    standby.refresh().await;
    assert!(
        standby.state().await.unwrap().stale,
        "a failed refresh keeps the last snapshot but marks it stale"
    );

    redis.start().await;
    standby.refresh().await;
    assert!(!standby.state().await.unwrap().stale);
}

#[tokio::test]
async fn standby_promotes_only_when_the_lease_is_free() {
    let redis = Redis::new().await;
    let store = redis.store().await;
    let lease = CoordinatorLease::new(&redis.url, "test", Duration::from_millis(300)).unwrap();

    // A live, held lease is never stolen: the standby observes it and stays
    // read-only until its stop signal.
    assert!(lease
        .acquire("primary-a", Duration::from_secs(60))
        .await
        .unwrap());
    let standby = Standby::new(store.clone());
    let promoted = tokio::time::timeout(
        Duration::from_secs(2),
        standby.run_until_promotion(
            &lease,
            "standby-b",
            Duration::from_secs(60),
            Duration::from_millis(50),
            &adx_coordinator::readiness::Readiness::new(),
            tokio::time::sleep(Duration::from_millis(300)),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!promoted);
    assert!(lease
        .renew("primary-a", 0, Duration::from_secs(60))
        .await
        .unwrap());

    // Once the primary lets the lease expire unrenewed, the standby observes
    // the expiry and competes for takeover.
    assert!(lease.release("primary-a").await.unwrap());
    assert!(lease
        .acquire("primary-a", Duration::from_millis(150))
        .await
        .unwrap());
    let promoted = tokio::time::timeout(
        Duration::from_secs(5),
        standby.run_until_promotion(
            &lease,
            "standby-b",
            Duration::from_millis(150),
            Duration::from_millis(50),
            &adx_coordinator::readiness::Readiness::new(),
            std::future::pending(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(promoted);
    // The promoted standby owns the lease now; the old primary does not.
    assert!(!lease
        .acquire("primary-c", Duration::from_secs(5))
        .await
        .unwrap());
    assert!(!lease
        .renew("primary-a", 0, Duration::from_secs(5))
        .await
        .unwrap());
    assert!(lease
        .renew("standby-b", 0, Duration::from_millis(500))
        .await
        .unwrap());
}
