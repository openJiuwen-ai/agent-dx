use adx_activator::registration::{RegistrationConfig, RegistrationLease};
use adx_agent_core::discovery::MemberCondition;
use adx_agent_store::discovery::RedisRegistry;
use std::sync::Arc;
use std::time::Duration;

fn condition_source() -> adx_activator::registration::ConditionSource {
    Arc::new(MemberCondition::default)
}

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
        condition_source(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(7)).await;
    assert_eq!(registry.members().await.unwrap().len(), 1);
    // AC-3: the unregister retry budget bounds the shutdown; a healthy Redis
    // unregisters well within it.
    lease.shutdown(Duration::from_secs(5)).await.unwrap();
    assert!(registry.members().await.unwrap().is_empty());
}

/// AC-4: the heartbeat renews with the current member condition, so a member
/// whose condition source reports draining is published as draining and the
/// Gateway-side selection filter can drop it before the lease is removed.
#[tokio::test]
#[ignore = "requires disposable ADX_AGENT_TEST_REDIS_URL"]
async fn heartbeat_carries_the_current_member_condition() {
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let namespace = format!("registration-condition-{}", uuid::Uuid::new_v4());
    let registry = RedisRegistry::new(&url, &namespace, Duration::from_secs(2)).unwrap();
    let condition = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let source: adx_activator::registration::ConditionSource = {
        let condition = condition.clone();
        Arc::new(move || MemberCondition {
            ready: true,
            draining: condition.load(std::sync::atomic::Ordering::Acquire),
        })
    };
    let lease = RegistrationLease::start(
        registry.clone(),
        RegistrationConfig {
            instance_id: "a".into(),
            advertise_url: "http://127.0.0.1:8091".into(),
            lease_seconds: 6,
            heartbeat_seconds: 1,
        },
        true,
        source,
    )
    .await
    .unwrap();
    assert!(!lease.condition().draining);
    // Wait for at least one heartbeat, flip the condition, and wait for the
    // next renewal to publish it.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    condition.store(true, std::sync::atomic::Ordering::Release);
    let observed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let members = registry.members().await.unwrap();
            if members.iter().any(|member| member.condition.draining) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        observed.is_ok(),
        "heartbeat publishes the draining condition"
    );
    lease.shutdown(Duration::from_secs(5)).await.unwrap();
    assert!(registry.members().await.unwrap().is_empty());
}
