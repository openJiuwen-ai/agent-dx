use adx_agent_core::discovery::ActivatorEndpoint;
use adx_agent_store::{discovery::RedisRegistry, Error};
use std::time::Duration;
#[tokio::test]
#[ignore = "requires ADX_AGENT_TEST_REDIS_URL pointing to a disposable Redis"]
async fn registry_leases_expire_and_old_incarnations_cannot_remove_replacements() {
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let namespace = format!("discovery-{}", uuid::Uuid::new_v4());
    let registry = RedisRegistry::new(&url, &namespace, Duration::from_secs(2)).unwrap();
    let endpoint = ActivatorEndpoint {
        id: "a".into(),
        url: "http://127.0.0.1:8091".into(),
    };
    registry
        .renew(&endpoint, "old", Duration::from_millis(100))
        .await
        .unwrap();
    assert_eq!(registry.members().await.unwrap(), vec![endpoint.clone()]);
    assert!(matches!(
        registry
            .renew(&endpoint, "new", Duration::from_secs(1))
            .await,
        Err(Error::Conflict(_))
    ));
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(registry.members().await.unwrap().is_empty());
    registry
        .renew(&endpoint, "new", Duration::from_secs(1))
        .await
        .unwrap();
    registry.unregister(&endpoint.id, "old").await.unwrap();
    assert_eq!(registry.members().await.unwrap(), vec![endpoint.clone()]);
    assert!(matches!(
        registry
            .renew(&endpoint, "old", Duration::from_secs(1))
            .await,
        Err(Error::Conflict(_))
    ));
    registry.unregister(&endpoint.id, "new").await.unwrap();
    assert!(registry.members().await.unwrap().is_empty());
}
