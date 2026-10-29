use adx_agent_core::alias::{AliasConfig, VersionWeight};
use adx_agent_store::{AgentState, MetadataChange, RedisRepository};
use std::{sync::Arc, time::Duration};

#[tokio::test]
#[ignore = "requires disposable Redis with notify-keyspace-events K$g"]
async fn subscription_observes_alias_and_binding_updates() {
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let namespace = format!("notifications-{}", uuid::Uuid::new_v4());
    let store = Arc::new(
        RedisRepository::connect(&url, &namespace, Duration::from_secs(3))
            .await
            .unwrap(),
    );
    let mut changes = store.subscribe_changes().await.unwrap();
    let state = AgentState::new(store);
    let template=serde_json::from_value(serde_json::json!({"name":"agent","version":"1","image":"agent:v1","isolation_runtime":"runsc","resources":{"cpu_millis":1000,"memory_mib":512}})).unwrap();
    state
        .publish("tenant", &adx_agent_core::TemplateDraft::from(&template))
        .await
        .unwrap();
    let config = AliasConfig {
        tenant: "tenant".into(),
        template: "agent".into(),
        alias: "production".into(),
        seed: "seed".into(),
        versions: vec![VersionWeight {
            version: "1".into(),
            weight: 1,
        }],
    };
    state.put_alias(&config, None).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), changes.recv())
            .await
            .unwrap()
            .unwrap(),
        MetadataChange::Alias {
            tenant: "tenant".into(),
            template: "agent".into(),
            alias: "production".into()
        }
    );
    let scope = adx_agent_core::Scope {
        tenant: "tenant".into(),
        template: "agent".into(),
        template_ref: adx_agent_core::TemplateRef::Alias("production".into()),
        binding_id: "workspace".into(),
    };
    state.create_binding(scope.clone()).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), changes.recv())
            .await
            .unwrap()
            .unwrap(),
        MetadataChange::Binding(scope)
    );
}
