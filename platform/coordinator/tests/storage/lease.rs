//! C-1: the Coordinator primary lease — acquire/renew/release CAS semantics,
//! expiry by Redis server time, and the crash window between acquire and
//! begin (epoch 0) recovering without manual cleanup.
use super::common::Redis;
use adx_coordinator::lease::CoordinatorLease;
use std::time::Duration;

fn lease(redis: &Redis) -> CoordinatorLease {
    CoordinatorLease::new(&redis.url, "lease-test", Duration::from_millis(300)).unwrap()
}

#[tokio::test]
async fn two_instances_compete_and_only_the_holder_renews() {
    let redis = Redis::new().await;
    let a = lease(&redis);
    let b = lease(&redis);

    // Only one of two competitors takes the lease.
    assert!(a
        .acquire("primary-a", Duration::from_millis(2_000))
        .await
        .unwrap());
    assert!(!b
        .acquire("primary-b", Duration::from_millis(2_000))
        .await
        .unwrap());

    // The holder binds its epoch through the first renew after begin;
    // before that the record shows the acquire→begin crash window.
    let view = a.observe().await.unwrap();
    assert!(view.live());
    assert_eq!(view.record.as_ref().unwrap().epoch, 0);
    assert_eq!(view.record.as_ref().unwrap().holder, "primary-a");

    assert!(a
        .renew("primary-a", 7, Duration::from_millis(2_000))
        .await
        .unwrap());
    let view = a.observe().await.unwrap();
    assert_eq!(view.record.as_ref().unwrap().epoch, 7);

    // Renew is CAS on holder AND epoch.
    assert!(!a
        .renew("primary-a", 8, Duration::from_millis(2_000))
        .await
        .unwrap());
    assert!(!b
        .renew("primary-b", 7, Duration::from_millis(2_000))
        .await
        .unwrap());
    // The epoch in the record survives renews.
    assert!(a
        .renew("primary-a", 7, Duration::from_millis(2_000))
        .await
        .unwrap());

    // Release only works for the holder and clears the key.
    assert!(!b.release("primary-b").await.unwrap());
    assert!(a.release("primary-a").await.unwrap());
    let view = a.observe().await.unwrap();
    assert!(!view.live());
    assert!(view.record.is_none());
    // Idempotent on an absent key; a release by the former holder of
    // someone else's new lease is refused.
    assert!(a.release("primary-a").await.unwrap());
    assert!(b
        .acquire("primary-b", Duration::from_millis(2_000))
        .await
        .unwrap());
    assert!(!a.release("primary-a").await.unwrap());
}

#[tokio::test]
async fn an_expired_lease_can_be_reacquired_but_not_resurrected_by_its_holder() {
    let redis = Redis::new().await;
    let a = lease(&redis);
    let b = lease(&redis);

    assert!(a
        .acquire("primary-a", Duration::from_millis(100))
        .await
        .unwrap());
    assert!(a
        .renew("primary-a", 5, Duration::from_millis(100))
        .await
        .unwrap());
    tokio::time::sleep(Duration::from_millis(150)).await;

    // The lease expired by server time: the standby can take over...
    let view = b.observe().await.unwrap();
    assert!(!view.live(), "expired lease is not live");
    assert!(b
        .acquire("primary-b", Duration::from_millis(2_000))
        .await
        .unwrap());
    // ...and the old holder cannot renew or release anymore.
    assert!(!a
        .renew("primary-a", 5, Duration::from_millis(2_000))
        .await
        .unwrap());
    assert!(!a.release("primary-a").await.unwrap());

    // A holder that missed its own deadline cannot resurrect the lease even
    // when no standby stepped in: renewal after expiry fails.
    assert!(b.release("primary-b").await.unwrap());
    assert!(a
        .acquire("primary-a", Duration::from_millis(100))
        .await
        .unwrap());
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!a
        .renew("primary-a", 9, Duration::from_millis(2_000))
        .await
        .unwrap());
    let view = a.observe().await.unwrap();
    assert!(!view.live());
}

#[tokio::test]
async fn the_crash_window_between_acquire_and_begin_needs_no_manual_cleanup() {
    let redis = Redis::new().await;
    let a = lease(&redis);
    let b = lease(&redis);

    // "a" acquires but crashes before begin: the record exists with epoch 0
    // and blocks "b" for at most one lease period.
    assert!(a
        .acquire("primary-a", Duration::from_millis(100))
        .await
        .unwrap());
    assert!(!b
        .acquire("primary-b", Duration::from_millis(100))
        .await
        .unwrap());
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(b
        .acquire("primary-b", Duration::from_millis(2_000))
        .await
        .unwrap());
    // The new holder binds a fresh epoch; the old one's renew is refused.
    assert!(b
        .renew("primary-b", 11, Duration::from_millis(2_000))
        .await
        .unwrap());
    assert!(!a
        .renew("primary-a", 0, Duration::from_millis(2_000))
        .await
        .unwrap());
}

#[tokio::test]
async fn holder_and_lease_bounds_are_validated() {
    assert!(CoordinatorLease::new("redis://127.0.0.1:1/", "", Duration::from_millis(100)).is_err());
    let lease =
        CoordinatorLease::new("redis://127.0.0.1:1/", "ns", Duration::from_millis(100)).unwrap();
    // Invalid inputs are rejected before any Redis round trip.
    lease
        .acquire("bad holder!", Duration::from_secs(1))
        .await
        .unwrap_err();
    lease
        .acquire("a", Duration::from_millis(10))
        .await
        .unwrap_err();
    lease
        .acquire("a", Duration::from_secs(700_000))
        .await
        .unwrap_err();
    assert!(adx_discovery::lease_key("bad ns").is_err());
    assert_eq!(
        adx_discovery::lease_key("ns").unwrap(),
        "adx:{ns}:coordinator:lease"
    );
}

#[tokio::test]
async fn supervision_renews_until_stopped_and_keeps_the_lease_alive() {
    let redis = Redis::new().await;
    let a = lease(&redis);

    // A 150ms lease expires without renewal; supervised renewal keeps it
    // alive across many renewal periods until the stop signal, and the
    // lease outlives the stop only by its remaining budget.
    assert!(a
        .acquire("primary-a", Duration::from_millis(150))
        .await
        .unwrap());
    assert!(a
        .renew("primary-a", 3, Duration::from_millis(150))
        .await
        .unwrap());
    let supervised = {
        let lease = a.clone();
        tokio::spawn(async move {
            lease
                .supervise(
                    "primary-a",
                    3,
                    Duration::from_millis(150),
                    tokio::time::sleep(Duration::from_millis(500)),
                )
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(400)).await;
    let view = a.observe().await.unwrap();
    assert!(view.live(), "supervised renewal keeps the lease alive");
    supervised.await.unwrap().unwrap();
    // Stopped while still held: the lease remains live until it expires,
    // and the holder can still release it.
    let view = a.observe().await.unwrap();
    assert!(view.live());
    assert!(a.release("primary-a").await.unwrap());
}

#[tokio::test]
async fn supervision_reports_a_definitive_loss_immediately() {
    let redis = Redis::new().await;
    let a = lease(&redis);

    assert!(a
        .acquire("primary-a", Duration::from_millis(2_000))
        .await
        .unwrap());
    assert!(a
        .renew("primary-a", 5, Duration::from_millis(2_000))
        .await
        .unwrap());
    let supervised = {
        let lease = a.clone();
        tokio::spawn(async move {
            lease
                .supervise(
                    "primary-a",
                    5,
                    Duration::from_millis(2_000),
                    std::future::pending::<()>(),
                )
                .await
        })
    };
    // Another holder takes over: the next supervised renewal is
    // definitively rejected (not a timeout), so the supervision must end
    // immediately instead of retrying or finishing "gracefully".
    assert!(a.release("primary-a").await.unwrap());
    assert!(a
        .acquire("primary-b", Duration::from_millis(2_000))
        .await
        .unwrap());
    let error = supervised.await.unwrap().unwrap_err();
    assert!(
        matches!(&error, adx_core::Error::Unavailable(message) if message.contains("lease lost")),
        "definitive loss must surface as unavailable-with-reason: {error:?}"
    );
}

#[tokio::test]
async fn supervision_retries_unavailability_within_the_lease_budget_then_stops() {
    let mut redis = Redis::new().await;
    let a = lease(&redis);

    assert!(a
        .acquire("primary-a", Duration::from_millis(300))
        .await
        .unwrap());
    assert!(a
        .renew("primary-a", 5, Duration::from_millis(300))
        .await
        .unwrap());
    redis.crash();
    // With Redis unavailable, renewals retry within the remaining lease
    // budget (anchored at the renewal tick) and then stop: a lease whose
    // budget is exhausted has factually expired server-side, so the
    // supervision must not outlive the primary's authority.
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        a.supervise(
            "primary-a",
            5,
            Duration::from_millis(300),
            std::future::pending::<()>(),
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(
        matches!(&error, adx_core::Error::Unavailable(message) if message.contains("lease budget")),
        "exhausted budget must surface as unavailable-with-reason: {error:?}"
    );
    redis.start().await;
}
