use adx_agent_store::*;
use std::time::Duration;

#[tokio::test]
#[ignore = "requires ADX_AGENT_TEST_REDIS_URL for a disposable real Redis"]
async fn expiry_and_old_boot_fencing() {
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").expect("set disposable Redis URL");
    let namespace = format!("members-{}", uuid::Uuid::new_v4());
    let a = RedisRepository::connect(&url, &namespace, Duration::from_secs(3))
        .await
        .unwrap();
    let b = RedisRepository::connect(&url, &namespace, Duration::from_secs(3))
        .await
        .unwrap();
    let old = DispatcherMember {
        node_id: "node".into(),
        boot_id: uuid::Uuid::new_v4().to_string(),
        address: "http://127.0.0.1:8080".into(),
    };
    let new = DispatcherMember {
        boot_id: uuid::Uuid::new_v4().to_string(),
        ..old.clone()
    };
    assert!(a
        .register_dispatcher(&old, Duration::from_secs(10))
        .await
        .unwrap());
    assert!(!b
        .register_dispatcher(&new, Duration::from_secs(10))
        .await
        .unwrap());
    assert!(!b
        .renew_dispatcher(&new, Duration::from_secs(10))
        .await
        .unwrap());
    assert!(a
        .renew_dispatcher(&old, Duration::from_millis(100))
        .await
        .unwrap());
    // Wait for actual server expiration, not a local authority lease.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !b
        .register_dispatcher(&new, Duration::from_secs(10))
        .await
        .unwrap()
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "old member did not expire"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(!a
        .renew_dispatcher(&old, Duration::from_secs(10))
        .await
        .unwrap());
    assert!(!a.unregister_dispatcher(&old).await.unwrap());
    assert_eq!(b.dispatchers().await.unwrap(), vec![new.clone()]);
    assert!(b.unregister_dispatcher(&new).await.unwrap());
    assert!(a.dispatchers().await.unwrap().is_empty());
}
