use adx_activator::registration::{RegistrationConfig, RegistrationLease};
use adx_agent_store::discovery::RedisRegistry;
use std::time::Duration;
#[tokio::test]
#[ignore = "requires disposable ADX_AGENT_TEST_REDIS_URL"]
async fn heartbeat_renews_an_idle_instance_and_shutdown_unregisters_it() {
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let namespace = format!("registration-{}", uuid::Uuid::new_v4());
    let registry = RedisRegistry::new(&url, &namespace, Duration::from_secs(2)).unwrap();
    let lease = RegistrationLease::start(
        registry.clone(),
        RegistrationConfig {
            instance_id: "a".into(),
            advertise_url: "http://127.0.0.1:8091".into(),
            lease_seconds: 6,
            heartbeat_seconds: 1,
        },
        true,
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(7)).await;
    assert_eq!(registry.members().await.unwrap().len(), 1);
    lease.shutdown().await.unwrap();
    assert!(registry.members().await.unwrap().is_empty());
}
