use adx_agent_core::{
    service_grants::{credential_digest, ServiceAccess},
    Scope, TemplateVersion,
};
use adx_agent_store::{AgentState, Repository};
use std::{collections::BTreeMap, sync::Arc};

async fn exercise(repository: Arc<dyn Repository>) {
    let state = AgentState::new(repository);
    let template: TemplateVersion = serde_json::from_value(serde_json::json!({
        "name":"orbit", "version":"1", "image":"registry/orbit:1", "isolation_runtime":"runsc",
        "resources":{"cpu_millis":1000,"memory_mib":512}
    }))
    .unwrap();
    state
        .publish(
            "digitalmate",
            &adx_agent_core::TemplateDraft::from(&template),
        )
        .await
        .unwrap();
    let scope = Scope {
        tenant: "digitalmate".into(),
        template: "orbit".into(),
        template_ref: adx_agent_core::TemplateRef::Version("1".into()),
        binding_id: "alice".into(),
    };
    let original = state
        .create_binding_with_parameters(
            scope.clone(),
            &BTreeMap::from([("workspace".into(), "w-alice".into())]),
        )
        .await
        .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let expected = ServiceAccess {
        tenant: "digitalmate".into(),
        workspace: "w-alice".into(),
        purpose: "orbit-config".into(),
    };
    let digest = credential_digest(&"a".repeat(43)).unwrap();
    assert!(state
        .create_service_grant(
            &scope,
            "wrong-generation",
            &digest,
            "orbit-config",
            now + 60,
            now
        )
        .await
        .is_err());
    let context = state
        .create_service_grant(
            &scope,
            &original.generation,
            &digest,
            "orbit-config",
            now + 60,
            now,
        )
        .await
        .unwrap();
    assert_eq!(context, expected);
    assert!(state
        .authorize_service_grant(&digest, &expected, now)
        .await
        .unwrap());
    assert!(!state
        .authorize_service_grant(
            &digest,
            &ServiceAccess {
                workspace: "w-bob".into(),
                ..expected.clone()
            },
            now
        )
        .await
        .unwrap());
    assert!(!state
        .authorize_service_grant(
            &digest,
            &ServiceAccess {
                tenant: "other".into(),
                ..expected.clone()
            },
            now
        )
        .await
        .unwrap());
    assert!(!state
        .authorize_service_grant(
            &digest,
            &ServiceAccess {
                purpose: "other".into(),
                ..expected.clone()
            },
            now
        )
        .await
        .unwrap());
    assert!(!state
        .authorize_service_grant(&digest, &expected, now + 61)
        .await
        .unwrap());
    assert!(state
        .create_service_grant(
            &scope,
            &original.generation,
            &digest,
            "orbit-config",
            now + 60,
            now
        )
        .await
        .is_err());
    state.revoke_service_grant(&digest).await.unwrap();
    assert!(!state
        .authorize_service_grant(&digest, &expected, now)
        .await
        .unwrap());
    state.revoke_service_grant(&digest).await.unwrap();

    let second = credential_digest(&"b".repeat(43)).unwrap();
    state
        .create_service_grant(
            &scope,
            &original.generation,
            &second,
            "orbit-config",
            now + 60,
            now,
        )
        .await
        .unwrap();
    let deleting = state.begin_delete(&scope).await.unwrap();
    assert!(!state
        .authorize_service_grant(&second, &expected, now)
        .await
        .unwrap());
    state.finish_delete(&deleting).await.unwrap();
    let fresh = state
        .create_binding_with_parameters(
            scope.clone(),
            &BTreeMap::from([("workspace".into(), "w-alice".into())]),
        )
        .await
        .unwrap();
    assert_ne!(original.generation, fresh.generation);
    assert!(!state
        .authorize_service_grant(&second, &expected, now)
        .await
        .unwrap());
}

#[cfg(feature = "test-memory")]
#[tokio::test]
async fn grants_derive_ownership_from_existing_binding_and_reject_revocation_or_replacement() {
    exercise(Arc::new(adx_agent_store::MemoryRepository::default())).await;
}

#[tokio::test]
#[ignore = "requires ADX_AGENT_TEST_REDIS_URL for an isolated Redis"]
async fn redis_grants_use_existing_repository_and_expire_without_scanning() {
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let namespace = format!("grants-{}", uuid::Uuid::new_v4());
    let repository = Arc::new(
        adx_agent_store::RedisRepository::connect(
            &url,
            &namespace,
            std::time::Duration::from_secs(3),
        )
        .await
        .unwrap(),
    );
    exercise(repository).await;
    let digest = credential_digest(&"b".repeat(43)).unwrap();
    let redis_key = format!(
        "adx:v2:{namespace}:service-grant:{}",
        adx_agent_core::encode_key(&[&digest])
    );
    let client = redis::Client::open(url).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let ttl: i64 = redis::cmd("TTL")
        .arg(&redis_key)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!((1..=60).contains(&ttl));
    let raw: String = redis::cmd("GET")
        .arg(redis_key)
        .query_async(&mut connection)
        .await
        .unwrap();
    let envelope: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert!(envelope["value"].get("workspace").is_none());
    assert_eq!(envelope["value"]["scope"]["binding_id"], "alice");
}
