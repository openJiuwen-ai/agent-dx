#![cfg(feature = "test-memory")]
use adx_agent_core::{
    alias::{AliasConfig, VersionWeight},
    *,
};
use adx_agent_store::{AgentState, MemoryRepository};
use std::sync::Arc;

async fn fixture() -> (AgentState, Scope, AliasConfig) {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    for version in ["1", "2", "3"] {
        let template = serde_json::from_value(serde_json::json!({
            "name":"agent", "version":version, "image":format!("agent:{version}"),
            "isolation_runtime":"runsc", "resources":{"cpu_millis":1000,"memory_mib":512}
        }))
        .unwrap();
        state
            .publish("tenant", &adx_agent_core::TemplateDraft::from(&template))
            .await
            .unwrap();
    }
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
    let scope = Scope {
        tenant: "tenant".into(),
        template: "agent".into(),
        binding_id: "workspace".into(),
        template_ref: TemplateRef::Alias("production".into()),
    };
    (state, scope, config)
}

async fn select(state: &AgentState, config: &mut AliasConfig, version: &str) {
    let (revision, _) = state
        .alias(&config.tenant, &config.template, &config.alias)
        .await
        .unwrap()
        .unwrap();
    config.versions = vec![VersionWeight {
        version: version.into(),
        weight: 1,
    }];
    state.put_alias(config, Some(&revision)).await.unwrap();
}

#[tokio::test]
async fn switch_is_atomic_and_retirement_survives_restart() {
    let (state, scope, mut config) = fixture().await;
    let original = state.create_binding(scope.clone()).await.unwrap();
    assert_eq!(original.current.version, "1");
    select(&state, &mut config, "2").await;
    let prepared = state.prepare_binding(&scope).await.unwrap();
    assert_eq!(prepared.current, original.current);
    assert_eq!(
        prepared.secondary.as_ref().unwrap().intent,
        SecondaryIntent::Preparing
    );
    let again = state.prepare_binding(&scope).await.unwrap();
    assert_eq!(
        again, prepared,
        "retries must retain identity and saved candidate"
    );
    let switched = state.switch_current(&prepared).await.unwrap();
    assert_eq!(switched.generation, original.generation);
    assert_eq!(switched.scope, original.scope);
    assert_eq!(switched.current.version, "2");
    let old = switched.secondary.as_ref().unwrap();
    assert_eq!(old.environment, original.current);
    assert_eq!(old.intent, SecondaryIntent::Retiring);
    assert_eq!(
        state.prepare_binding(&scope).await.unwrap(),
        switched,
        "matching current must not erase pending retirement"
    );
    state.clear_secondary(&switched).await.unwrap();
    assert!(state
        .binding(&scope)
        .await
        .unwrap()
        .unwrap()
        .secondary
        .is_none());
}

#[tokio::test]
async fn latest_config_fences_promotion_and_a_third_environment() {
    let (state, scope, mut config) = fixture().await;
    state.create_binding(scope.clone()).await.unwrap();
    select(&state, &mut config, "2").await;
    let prepared = state.prepare_binding(&scope).await.unwrap();
    select(&state, &mut config, "3").await;
    assert!(state.switch_current(&prepared).await.is_err());
    let retiring = state.prepare_binding(&scope).await.unwrap();
    assert_eq!(retiring.current.version, "1");
    assert_eq!(
        retiring.secondary.as_ref().unwrap().environment.version,
        "2"
    );
    assert_eq!(
        retiring.secondary.as_ref().unwrap().intent,
        SecondaryIntent::Retiring
    );
    assert_eq!(state.prepare_binding(&scope).await.unwrap(), retiring);
    state.clear_secondary(&retiring).await.unwrap();
    let next = state.prepare_binding(&scope).await.unwrap();
    assert_eq!(next.secondary.unwrap().environment.version, "3");
    assert!(
        state.clear_secondary(&retiring).await.is_err(),
        "late cleanup cannot erase new candidate"
    );
}

#[tokio::test]
async fn concurrent_preparation_uses_one_persisted_identity() {
    let (state, scope, mut config) = fixture().await;
    state.create_binding(scope.clone()).await.unwrap();
    select(&state, &mut config, "2").await;
    let (a, b) = tokio::join!(state.prepare_binding(&scope), state.prepare_binding(&scope));
    assert_eq!(a.unwrap(), b.unwrap());
}

#[tokio::test]
async fn alias_updates_require_expected_revision_and_published_versions() {
    let (state, _, mut config) = fixture().await;
    let (revision, _) = state
        .alias("tenant", "agent", "production")
        .await
        .unwrap()
        .unwrap();
    assert!(state.put_alias(&config, None).await.is_err());
    select(&state, &mut config, "2").await;
    assert!(state.put_alias(&config, Some(&revision)).await.is_err());
    config.alias = "missing-version".into();
    config.versions[0].version = "unpublished".into();
    assert!(state.put_alias(&config, None).await.is_err());
}

#[tokio::test]
async fn creation_cancellation_cas_advances_once_and_backoff_is_bounded() {
    let (state, scope, mut config) = fixture().await;
    state.create_binding(scope.clone()).await.unwrap();
    select(&state, &mut config, "2").await;
    let saved = state.prepare_binding(&scope).await.unwrap();
    let candidate = &saved.secondary.as_ref().unwrap().environment;
    let attempt = CreationAttempt::default();
    let (a, b) = tokio::join!(
        state.defer_creation(&saved, candidate, &attempt, true, 1000),
        state.defer_creation(&saved, candidate, &attempt, true, 1000)
    );
    assert_eq!(a.unwrap(), b.unwrap());
    let mut saved = state.binding(&scope).await.unwrap().unwrap();
    assert_eq!(saved.creation_attempts[&candidate.environment_id].number, 1);
    for expected in [10, 20, 40, 60, 60] {
        let previous = saved.creation_attempts[&candidate.environment_id].clone();
        saved = state
            .defer_creation(&saved, candidate, &previous, true, 1000)
            .await
            .unwrap();
        let next = &saved.creation_attempts[&candidate.environment_id];
        assert_eq!(next.number, previous.number + 1);
        assert_eq!(next.backoff_seconds, expected);
        assert_eq!(next.retry_after_unix_ms, 1000 + expected * 1000);
    }
    let (rev, mut config) = state
        .alias("tenant", "agent", "production")
        .await
        .unwrap()
        .unwrap();
    config.versions[0].version = "1".into();
    state.put_alias(&config, Some(&rev)).await.unwrap();
    state.prepare_binding(&scope).await.unwrap();
    let previous = saved.creation_attempts[&candidate.environment_id].clone();
    assert!(state
        .defer_creation(&saved, candidate, &previous, true, 1000)
        .await
        .is_err());
}
