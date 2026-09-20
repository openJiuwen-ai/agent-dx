#[path = "../../../tests/pool_fixture.rs"]
mod pool_fixture;
use adx_agent_core::*;
use adx_agent_store::*;
use std::sync::Arc;

async fn exercise(a: Arc<dyn Repository>, b: Arc<dyn Repository>) {
    let repo = a.clone();
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
        session_id: "session".into(),
    };
    a.create_session(scope.clone()).await.unwrap();
    b.create_session(scope.clone()).await.unwrap();
    let (x, y) = tokio::join!(
        a.reserve_cold_start(&scope, "i-a", "sandbox-a"),
        b.reserve_cold_start(&scope, "i-b", "sandbox-b")
    );
    let winner = match (x.unwrap(), y.unwrap()) {
        (Reservation::Created(id), Reservation::PoolExists(ids))
        | (Reservation::PoolExists(ids), Reservation::Created(id)) => {
            assert_eq!(ids, vec![id.clone()]);
            id
        }
        other => panic!("invalid cold-start race: {other:?}"),
    };
    let loser = if winner == "i-a" { "i-b" } else { "i-a" };
    assert!(b.instance("tenant", loser).await.unwrap().is_none());
    assert!(a.bind(&scope, "affinity", &winner).await.is_err()); // Creating isn't Ready.
    a.mark_ready("tenant", &winner).await.unwrap();
    let (x, y) = tokio::join!(
        a.bind(&scope, "affinity", &winner),
        b.bind(&scope, "affinity", &winner)
    );
    assert_eq!(x.unwrap(), y.unwrap());
    // A delayed startup failure cannot regress a completed creation or its binding.
    a.mark_failed("tenant", &winner, "temporarily unavailable".into())
        .await
        .unwrap();
    assert_eq!(
        a.instance("tenant", &winner).await.unwrap().unwrap().phase,
        InstancePhase::Ready
    );
    assert_eq!(
        b.bind(&scope, "affinity", loser).await.unwrap().instance_id,
        winner
    );
    a.mark_ready("tenant", &winner).await.unwrap();
    // Deletion in progress must not detach affinity.
    a.begin_delete("tenant", &winner).await.unwrap();
    assert!(b.bind(&scope, "new-affinity", &winner).await.is_err());
    assert!(b.bind(&scope, "affinity", loser).await.is_err());
    a.confirm_deleted("tenant", &winner).await.unwrap();
    a.reserve_cold_start(&scope, loser, "replacement-sandbox")
        .await
        .unwrap();
    a.mark_ready("tenant", loser).await.unwrap();
    assert_eq!(
        b.bind(&scope, "affinity", loser).await.unwrap().instance_id,
        loser
    );

    let mut other_scope = scope.clone();
    other_scope.tenant = "another-tenant".into();
    assert!(a.bind(&other_scope, "affinity", &winner).await.is_err());

    let released = a.release_session(&scope).await.unwrap();
    assert_eq!(released.phase, SessionPhase::Deleting);
    assert!(b
        .reserve_cold_start(&scope, "late", "late-sandbox")
        .await
        .is_err());
    assert!(a.confirm_deleted("tenant", loser).await.is_err());
    a.begin_delete("tenant", loser).await.unwrap();
    assert!(b.mark_ready("tenant", loser).await.is_err());
    a.confirm_deleted("tenant", loser).await.unwrap();
    assert!(a
        .finish_session_delete(&scope, &released.generation)
        .await
        .unwrap());
    assert!(b.session(&scope).await.unwrap().is_none());
    assert!(a
        .affinity_in_session(&scope, &released.generation, "affinity")
        .await
        .unwrap()
        .is_none());
    a.create_session(scope.clone()).await.unwrap();
    let recreated = b.session(&scope).await.unwrap().unwrap();
    assert_ne!(recreated.generation, released.generation);
    assert!(a
        .reserve_cold_start_in_session(
            &scope,
            &released.generation,
            "stale",
            "stale",
            adx_agent_core::unix_time_millis()
                + adx_agent_core::limits::CREATE_TIMEOUT.as_millis() as u64
        )
        .await
        .is_err());
    assert!(a
        .bind_in_session(&scope, &released.generation, "affinity", loser)
        .await
        .is_err());
    // Old cleanup cannot remove the new lifecycle with the same public ID.
    a.finish_session_delete(&scope, &released.generation)
        .await
        .unwrap();
    assert_eq!(
        b.session(&scope).await.unwrap().unwrap().generation,
        recreated.generation
    );

    let mut pool = scope.clone();
    pool.session_id = "multi-instance".into();
    a.create_session(pool.clone()).await.unwrap();
    for id in ["pool-a", "pool-b"] {
        pool_fixture::insert_instance(repo.as_ref(), &pool, id, InstancePhase::Ready).await;
    }
    let (x, y) = tokio::join!(
        a.bind(&pool, "race", "pool-a"),
        b.bind(&pool, "race", "pool-b")
    );
    let bound = x.unwrap();
    assert_eq!(bound, y.unwrap());
    assert!(["pool-a", "pool-b"].contains(&bound.instance_id.as_str()));
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
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").expect("set disposable Redis URL");
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
async fn deletion_cas_retry_cannot_delete_recreated_session() {
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
        async fn scan(&self, k: &str, c: u64, n: u32) -> Result<Page> {
            self.inner.scan(k, c, n).await
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
        session_id: "reused".into(),
    };
    state.create_session(scope.clone()).await.unwrap();
    repo.pause.store(true, Ordering::SeqCst);
    let deleting = tokio::spawn({
        let state = state.clone();
        let scope = scope.clone();
        async move { state.release_session(&scope).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), repo.entered.notified())
        .await
        .unwrap();
    let old = state.release_session(&scope).await.unwrap();
    state
        .finish_session_delete(&scope, &old.generation)
        .await
        .unwrap();
    state.create_session(scope.clone()).await.unwrap();
    let fresh = state.session(&scope).await.unwrap().unwrap();
    repo.resume.notify_one();
    assert!(matches!(deleting.await.unwrap(), Err(Error::Conflict(_))));
    assert_eq!(state.session(&scope).await.unwrap().unwrap(), fresh);
}
