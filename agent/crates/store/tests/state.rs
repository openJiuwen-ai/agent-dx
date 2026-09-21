use adx_agent_core::*;
use adx_agent_store::*;
use std::sync::Arc;
async fn exercise(a: Arc<dyn Repository>, b: Arc<dyn Repository>) {
    let a = AgentState::new(a);
    let b = AgentState::new(b);
    let mut template: TemplateVersion = serde_json::from_value(serde_json::json!({
        "name":"agent", "version":"1", "image":"registry/agent:1", "isolation_runtime":"runsc",
        "entrypoint":["/app/start"], "resources":{"cpu_millis":1000,"memory_mib":512}
    }))
    .unwrap();
    a.publish("tenant", &template).await.unwrap();
    b.publish("tenant", &template).await.unwrap();
    template.image = "registry/other:1".into();
    assert!(matches!(
        b.publish("tenant", &template).await,
        Err(Error::Conflict(_))
    ));
    let scope = Scope {
        tenant: "tenant".into(),
        template: "agent".into(),
        version: "1".into(),
        environment_id: "env".into(),
    };
    let (x, y) = tokio::join!(
        a.create_environment(scope.clone()),
        b.create_environment(scope.clone())
    );
    let original = x.unwrap();
    assert_eq!(original, y.unwrap());
    assert_eq!(a.environment(&scope).await.unwrap(), Some(original.clone()));
    let deleting = b.begin_delete(&scope).await.unwrap();
    assert_eq!(deleting.sandbox_id, original.sandbox_id);
    assert_eq!(deleting.phase, EnvironmentPhase::Deleting);
    assert!(matches!(
        a.create_environment(scope.clone()).await,
        Err(Error::Conflict(_))
    ));
    a.finish_delete(&deleting).await.unwrap();
    assert!(b.environment(&scope).await.unwrap().is_none());
    let fresh = b.create_environment(scope.clone()).await.unwrap();
    assert_ne!(fresh.generation, original.generation);
    assert_ne!(fresh.sandbox_id, original.sandbox_id);
    a.finish_delete(&deleting).await.unwrap();
    assert_eq!(a.environment(&scope).await.unwrap(), Some(fresh));
    let other = Scope {
        tenant: "other".into(),
        ..scope
    };
    assert!(a.environment(&other).await.unwrap().is_none());
}
#[cfg(feature = "test-memory")]
#[tokio::test]
async fn memory_product_transactions() {
    let store = Arc::new(MemoryRepository::default());
    exercise(store.clone(), store).await;
}
#[tokio::test]
#[ignore = "requires ADX_AGENT_TEST_REDIS_URL for a disposable real Redis"]
async fn independent_redis_product_transactions() {
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let namespace = format!("state-{}", uuid::Uuid::new_v4());
    let timeout = std::time::Duration::from_secs(3);
    let a = Arc::new(
        RedisRepository::connect(&url, &namespace, timeout)
            .await
            .unwrap(),
    );
    let b = Arc::new(
        RedisRepository::connect(&url, &namespace, timeout)
            .await
            .unwrap(),
    );
    exercise(a, b).await;
}
#[cfg(feature = "test-memory")]
#[tokio::test]
async fn deletion_cas_retry_cannot_delete_recreated_environment() {
    use std::sync::atomic::{AtomicBool, Ordering};
    #[derive(Default)]
    struct Paused {
        inner: MemoryRepository,
        pause: AtomicBool,
        entered: tokio::sync::Notify,
        resume: tokio::sync::Notify,
    }
    #[async_trait::async_trait]
    impl Repository for Paused {
        async fn get(&self, k: &Key) -> Result<Option<Record>> {
            self.inner.get(k).await
        }
        async fn commit(&self, t: &Transaction) -> Result<bool> {
            if self.pause.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            self.inner.commit(t).await
        }
    }
    let repo = Arc::new(Paused::default());
    let state = AgentState::new(repo.clone());
    let template=serde_json::from_value(serde_json::json!({"name":"test","version":"1","image":"app:1","isolation_runtime":"runc","entrypoint":["/start"],"resources":{"cpu_millis":1000,"memory_mib":512}})).unwrap();
    state.publish("tenant", &template).await.unwrap();
    let scope = Scope {
        tenant: "tenant".into(),
        template: "test".into(),
        version: "1".into(),
        environment_id: "reused".into(),
    };
    state.create_environment(scope.clone()).await.unwrap();
    repo.pause.store(true, Ordering::SeqCst);
    let deleting = tokio::spawn({
        let state = state.clone();
        let scope = scope.clone();
        async move { state.begin_delete(&scope).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), repo.entered.notified())
        .await
        .unwrap();
    let old = state.begin_delete(&scope).await.unwrap();
    state.finish_delete(&old).await.unwrap();
    state.create_environment(scope.clone()).await.unwrap();
    let fresh = state.environment(&scope).await.unwrap().unwrap();
    repo.resume.notify_one();
    assert!(matches!(deleting.await.unwrap(), Err(Error::Conflict(_))));
    assert_eq!(state.environment(&scope).await.unwrap().unwrap(), fresh);
}
