use adx_agent_core::*;
use adx_agent_store::*;
use std::sync::Arc;
async fn exercise(a: Arc<dyn Repository>, b: Arc<dyn Repository>) {
    let a = AgentState::new(a);
    let b = AgentState::new(b);
    let mut template: TemplateVersion = serde_json::from_value(serde_json::json!({
        "name":"agent", "version":"1", "image":"registry/agent:1", "isolation_runtime":"runsc",
        "resources":{"cpu_millis":1000,"memory_mib":512}
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
        binding_id: "env".into(),
    };
    let (x, y) = tokio::join!(
        a.create_binding(scope.clone()),
        b.create_binding(scope.clone())
    );
    let original = x.unwrap();
    assert_eq!(original, y.unwrap());
    assert_eq!(a.binding(&scope).await.unwrap(), Some(original.clone()));
    let deleting = b.begin_delete(&scope).await.unwrap();
    assert_eq!(deleting.sandbox_id, original.sandbox_id);
    assert_eq!(deleting.phase, BindingPhase::Deleting);
    assert!(matches!(
        a.create_binding(scope.clone()).await,
        Err(Error::Conflict(_))
    ));
    a.finish_delete(&deleting).await.unwrap();
    assert!(b.binding(&scope).await.unwrap().is_none());
    let fresh = b.create_binding(scope.clone()).await.unwrap();
    assert_ne!(fresh.generation, original.generation);
    assert_ne!(fresh.sandbox_id, original.sandbox_id);
    a.finish_delete(&deleting).await.unwrap();
    assert_eq!(a.binding(&scope).await.unwrap(), Some(fresh));
    let other = Scope {
        tenant: "other".into(),
        ..scope
    };
    assert!(a.binding(&other).await.unwrap().is_none());
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
async fn deletion_cas_retry_cannot_delete_recreated_binding() {
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
        async fn page(
            &self,
            index: &Index,
            after: Option<&str>,
            limit: usize,
        ) -> Result<Vec<(String, Record)>> {
            self.inner.page(index, after, limit).await
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
    let template=serde_json::from_value(serde_json::json!({"name":"test","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512}})).unwrap();
    state.publish("tenant", &template).await.unwrap();
    let scope = Scope {
        tenant: "tenant".into(),
        template: "test".into(),
        version: "1".into(),
        binding_id: "reused".into(),
    };
    state.create_binding(scope.clone()).await.unwrap();
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
    state.create_binding(scope.clone()).await.unwrap();
    let fresh = state.binding(&scope).await.unwrap().unwrap();
    repo.resume.notify_one();
    assert!(matches!(deleting.await.unwrap(), Err(Error::Conflict(_))));
    assert_eq!(state.binding(&scope).await.unwrap().unwrap(), fresh);
}

async fn binding_pages(repo: Arc<dyn Repository>) {
    use adx_agent_core::activator::BindingList;
    let state = AgentState::new(repo);
    let template = serde_json::from_value(serde_json::json!({"name":"app","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512}})).unwrap();
    for tenant in ["a", "b"] {
        state.publish(tenant, &template).await.unwrap();
        for id in ["one", "two", "three"] {
            state
                .create_binding(Scope {
                    tenant: tenant.into(),
                    template: "app".into(),
                    version: "1".into(),
                    binding_id: id.into(),
                })
                .await
                .unwrap();
        }
    }
    let query = BindingList {
        tenant: "a".into(),
        template: "app".into(),
        version: "1".into(),
        page_size: 2,
        page_token: None,
    };
    let first = state.list_bindings(&query).await.unwrap();
    assert_eq!(first.bindings.len(), 2);
    assert!(first.bindings.iter().all(|e| e.scope.tenant == "a"));
    let mut next = BindingList {
        page_token: first.next_page_token.clone(),
        ..query.clone()
    };
    let last = state.list_bindings(&next).await.unwrap();
    assert_eq!(last.bindings.len(), 1);
    assert!(last.next_page_token.is_none());
    assert!(!first.bindings.contains(&last.bindings[0]));
    next.tenant = "b".into();
    assert!(matches!(
        state.list_bindings(&next).await,
        Err(Error::Invalid(_))
    ));
    let deleting = state.begin_delete(&first.bindings[0].scope).await.unwrap();
    let full = BindingList {
        page_size: 50,
        ..query.clone()
    };
    assert!(state
        .list_bindings(&full)
        .await
        .unwrap()
        .bindings
        .contains(&deleting));
    state.finish_delete(&deleting).await.unwrap();
    let remaining = state.list_bindings(&full).await.unwrap();
    assert_eq!(remaining.bindings.len(), 2);
    let fresh = state.create_binding(deleting.scope.clone()).await.unwrap();
    state.finish_delete(&deleting).await.unwrap();
    assert!(state
        .list_bindings(&full)
        .await
        .unwrap()
        .bindings
        .contains(&fresh));
    assert!(state
        .list_bindings(&BindingList {
            page_token: Some("invalid".into()),
            ..query.clone()
        })
        .await
        .is_err());
    assert!(state
        .list_bindings(&BindingList {
            page_size: 0,
            ..query
        })
        .await
        .is_err());
}

#[cfg(feature = "test-memory")]
#[tokio::test]
async fn memory_binding_pagination() {
    binding_pages(Arc::new(MemoryRepository::default())).await;
}

#[tokio::test]
#[ignore = "requires ADX_AGENT_TEST_REDIS_URL for a disposable real Redis"]
async fn redis_binding_pagination() {
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let namespace = format!("pages-{}", uuid::Uuid::new_v4());
    binding_pages(Arc::new(
        RedisRepository::connect(&url, &namespace, std::time::Duration::from_secs(3))
            .await
            .unwrap(),
    ))
    .await;
}
