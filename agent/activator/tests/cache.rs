use adx_activator::{Activator, Error};
use adx_agent_core::{sandbox::*, *};
use adx_agent_store::{AgentState, Index, Key, MemoryRepository, Record, Repository, Transaction};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Default)]
struct Store {
    inner: MemoryRepository,
    reads: AtomicUsize,
    unavailable: AtomicBool,
}
#[async_trait::async_trait]
impl Repository for Store {
    async fn get(&self, key: &Key) -> adx_agent_store::Result<Option<Record>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(adx_agent_store::Error::Unavailable(
                "test repository unavailable".into(),
            ));
        }
        tokio::task::yield_now().await;
        self.inner.get(key).await
    }
    async fn commit(&self, tx: &Transaction) -> adx_agent_store::Result<bool> {
        self.inner.commit(tx).await
    }
    async fn page(
        &self,
        index: &Index,
        after: Option<&str>,
        limit: usize,
    ) -> adx_agent_store::Result<Vec<(String, Record)>> {
        self.inner.page(index, after, limit).await
    }
}

#[derive(Default)]
struct Platform {
    records: Mutex<HashMap<String, SandboxObservation>>,
    gets: AtomicUsize,
    pause_next_get: AtomicBool,
    get_started: tokio::sync::Notify,
    release_get: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl Sandbox for Platform {
    async fn create(&self, r: &CreateSandbox) -> Result<SandboxObservation, SandboxError> {
        let record = SandboxObservation {
            id: r.id.clone(),
            tenant: r.tenant.clone(),
            phase: SandboxPhase::Running,
            ready: true,
            runtime_id: None,
            message: None,
        };
        self.records
            .lock()
            .unwrap()
            .insert(r.id.clone(), record.clone());
        Ok(record)
    }
    async fn get(&self, _: &str, id: &str) -> Result<Option<SandboxObservation>, SandboxError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        let record = self.records.lock().unwrap().get(id).cloned();
        if self.pause_next_get.swap(false, Ordering::SeqCst) {
            self.get_started.notify_one();
            self.release_get.notified().await;
        }
        Ok(record)
    }
    async fn delete(&self, tenant: &str, id: &str) -> Result<SandboxObservation, SandboxError> {
        self.records.lock().unwrap().remove(id);
        Ok(SandboxObservation {
            id: id.into(),
            tenant: tenant.into(),
            phase: SandboxPhase::Deleted,
            ready: false,
            runtime_id: None,
            message: None,
        })
    }
}
fn template() -> TemplateVersion {
    serde_json::from_value(serde_json::json!({"name":"app","version":"1","image":"app:1","isolation_runtime":"runc","entrypoint":["/start"],"resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080}]})).unwrap()
}
fn scope() -> Scope {
    Scope {
        tenant: "tenant".into(),
        template: "app".into(),
        version: "1".into(),
        environment_id: "env".into(),
    }
}
fn deadline() -> u64 {
    unix_time_millis() + 60_000
}

#[tokio::test]
async fn warm_activation_skips_redis_and_platform() {
    let store = Arc::new(Store::default());
    let platform = Arc::new(Platform::default());
    let service = Activator::new(AgentState::new(store.clone()), platform.clone());
    service.publish("tenant", &template()).await.unwrap();
    let first = service.activate(&scope(), None, deadline()).await.unwrap();
    store.reads.store(0, Ordering::SeqCst);
    platform.gets.store(0, Ordering::SeqCst);
    service.template("tenant", "app", "1").await.unwrap();
    assert_eq!(
        service.activate(&scope(), None, deadline()).await.unwrap(),
        first
    );
    assert_eq!(
        platform.gets.load(Ordering::SeqCst),
        0,
        "warm activation must not query Sandbox"
    );
    assert_eq!(
        store.reads.load(Ordering::SeqCst),
        0,
        "warm activation must not query Redis"
    );
}

#[tokio::test]
async fn immutable_templates_share_cached_reads_without_caching_absence() {
    let store = Arc::new(Store::default());
    let state = AgentState::new(store.clone());
    assert!(state
        .template("tenant", "app", "1")
        .await
        .unwrap()
        .is_none());
    state.publish("tenant", &template()).await.unwrap();
    store.reads.store(0, Ordering::SeqCst);
    let (a, b) = tokio::join!(
        state.template("tenant", "app", "1"),
        state.template("tenant", "app", "1")
    );
    assert_eq!(a.unwrap(), Some(template()));
    assert_eq!(b.unwrap(), Some(template()));
    assert_eq!(
        store.reads.load(Ordering::SeqCst),
        1,
        "concurrent template misses share one load"
    );
    store.reads.store(0, Ordering::SeqCst);
    assert_eq!(
        state.template("tenant", "app", "1").await.unwrap(),
        Some(template())
    );
    state.create_environment(scope()).await.unwrap();
    assert_eq!(
        store.reads.load(Ordering::SeqCst),
        0,
        "create reuses the immutable template too"
    );
    assert!(state.template("other", "app", "1").await.unwrap().is_none());
}

#[tokio::test]
async fn bypasscache_refreshes_and_failed_refresh_does_not_leave_a_success_hit() {
    const TOKEN: &str = "test-activator-token-at-least-32-bytes";
    let platform = Arc::new(Platform::default());
    let service = Arc::new(Activator::new(
        AgentState::new(Arc::new(Store::default())),
        platform.clone(),
    ));
    service.publish("tenant", &template()).await.unwrap();
    let first = service.activate(&scope(), None, deadline()).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://{}/internal/adx/v1/environments/activate",
        listener.local_addr().unwrap()
    );
    let app =
        adx_activator::server::router(service.clone(), TOKEN, Duration::from_secs(3)).unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let response = reqwest::Client::new()
        .post(&url)
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({"scope":scope(),"bypasscache":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(platform.gets.load(Ordering::SeqCst), 2);
    platform
        .records
        .lock()
        .unwrap()
        .get_mut(&first.environment.sandbox_id)
        .unwrap()
        .ready = false;
    let response = reqwest::Client::new()
        .post(&url)
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({"scope":scope(),"bypasscache":true}))
        .send()
        .await
        .unwrap();
    assert!(matches!(
        response.json::<Error>().await.unwrap(),
        Error::NotReady(_)
    ));
    assert!(matches!(
        service.activate(&scope(), None, deadline()).await,
        Err(Error::NotReady(_))
    ));
    assert_eq!(platform.gets.load(Ordering::SeqCst), 4);
    task.abort();
}

#[tokio::test]
async fn remote_deletion_allows_stale_hits_until_bypass_and_preserves_generation() {
    let store = Arc::new(Store::default());
    let platform = Arc::new(Platform::default());
    let a = Activator::new(AgentState::new(store.clone()), platform.clone());
    let b = Activator::new(AgentState::new(store.clone()), platform.clone());
    a.publish("tenant", &template()).await.unwrap();
    let first = a.activate(&scope(), None, deadline()).await.unwrap();
    AgentState::new(store).begin_delete(&scope()).await.unwrap();
    assert_eq!(a.activate(&scope(), None, deadline()).await.unwrap(), first);
    assert!(matches!(
        a.activate_with_cache(
            &scope(),
            Some(&first.environment.generation),
            deadline(),
            true
        )
        .await,
        Err(Error::Conflict(_))
    ));
    b.delete_environment(&scope()).await.unwrap();
    let next = b.activate(&scope(), None, deadline()).await.unwrap();
    assert_ne!(first.environment.generation, next.environment.generation);
    assert!(matches!(
        a.activate_with_cache(
            &scope(),
            Some(&first.environment.generation),
            deadline(),
            true
        )
        .await,
        Err(Error::Conflict(_))
    ));
    let before = platform.gets.load(Ordering::SeqCst);
    assert_eq!(a.activate(&scope(), None, deadline()).await.unwrap(), next);
    assert_eq!(platform.gets.load(Ordering::SeqCst), before + 1);
}

#[tokio::test]
async fn older_observation_cannot_refill_cache_after_newer_refresh_fails() {
    let platform = Arc::new(Platform::default());
    let service = Arc::new(Activator::new(
        AgentState::new(Arc::new(Store::default())),
        platform.clone(),
    ));
    service.publish("tenant", &template()).await.unwrap();
    let first = service.activate(&scope(), None, deadline()).await.unwrap();
    platform.pause_next_get.store(true, Ordering::SeqCst);
    let older_service = service.clone();
    let older = tokio::spawn(async move {
        older_service
            .activate_with_cache(&scope(), None, deadline(), true)
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), platform.get_started.notified())
        .await
        .unwrap();
    platform
        .records
        .lock()
        .unwrap()
        .get_mut(&first.environment.sandbox_id)
        .unwrap()
        .ready = false;
    assert!(matches!(
        service
            .activate_with_cache(&scope(), None, deadline(), true)
            .await,
        Err(Error::NotReady(_))
    ));
    platform.release_get.notify_one();
    assert_eq!(older.await.unwrap().unwrap(), first);
    assert!(matches!(
        service.activate(&scope(), None, deadline()).await,
        Err(Error::NotReady(_))
    ));
    assert_eq!(platform.gets.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn not_ready_binding_is_not_cached_and_can_become_ready() {
    let state = AgentState::new(Arc::new(Store::default()));
    state.publish("tenant", &template()).await.unwrap();
    let environment = state.create_environment(scope()).await.unwrap();
    let platform = Arc::new(Platform::default());
    platform.records.lock().unwrap().insert(
        environment.sandbox_id.clone(),
        SandboxObservation {
            id: environment.sandbox_id.clone(),
            tenant: environment.scope.tenant.clone(),
            phase: SandboxPhase::Creating,
            ready: false,
            runtime_id: None,
            message: None,
        },
    );
    let service = Activator::new(state, platform.clone());
    assert!(matches!(
        service.activate(&scope(), None, deadline()).await,
        Err(Error::NotReady(_))
    ));
    {
        let mut records = platform.records.lock().unwrap();
        let record = records.get_mut(&environment.sandbox_id).unwrap();
        record.phase = SandboxPhase::Running;
        record.ready = true;
    }
    let ready = service.activate(&scope(), None, deadline()).await.unwrap();
    assert_eq!(
        service.activate(&scope(), None, deadline()).await.unwrap(),
        ready
    );
    assert_eq!(platform.gets.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn warm_cache_survives_redis_failure_but_bypass_invalidates_before_read() {
    let store = Arc::new(Store::default());
    let platform = Arc::new(Platform::default());
    let service = Activator::new(AgentState::new(store.clone()), platform.clone());
    service.publish("tenant", &template()).await.unwrap();
    store.unavailable.store(true, Ordering::SeqCst);
    assert!(matches!(
        service.template("tenant", "app", "1").await,
        Err(Error::Unavailable(_))
    ));
    store.unavailable.store(false, Ordering::SeqCst);
    let first = service.activate(&scope(), None, deadline()).await.unwrap();
    store.unavailable.store(true, Ordering::SeqCst);
    assert_eq!(
        service.activate(&scope(), None, deadline()).await.unwrap(),
        first
    );
    assert!(matches!(
        service
            .activate_with_cache(&scope(), None, deadline(), true)
            .await,
        Err(Error::Unavailable(_))
    ));
    assert!(matches!(
        service.activate(&scope(), None, deadline()).await,
        Err(Error::Unavailable(_))
    ));
    assert_eq!(platform.gets.load(Ordering::SeqCst), 1);
    store.unavailable.store(false, Ordering::SeqCst);
    assert_eq!(
        service.activate(&scope(), None, deadline()).await.unwrap(),
        first
    );
    assert_eq!(platform.gets.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn sliding_ttl_renews_on_access_and_lru_eviction_only_causes_reload() {
    let store = Arc::new(Store::default());
    let platform = Arc::new(Platform::default());
    let service = Activator::with_cache(
        AgentState::new(store.clone()),
        platform.clone(),
        adx_activator::CacheSettings {
            capacity: 2,
            idle_seconds: 10,
        },
    )
    .unwrap();
    service.publish("tenant", &template()).await.unwrap();
    let a = scope();
    let b = Scope {
        environment_id: "b".into(),
        ..scope()
    };
    let c = Scope {
        environment_id: "c".into(),
        ..scope()
    };
    let first = service.activate(&a, None, deadline()).await.unwrap();
    service.activate(&b, None, deadline()).await.unwrap();
    store.reads.store(0, Ordering::SeqCst);
    for _ in 0..3 {
        tokio::time::advance(Duration::from_secs(9)).await;
        assert_eq!(service.activate(&a, None, deadline()).await.unwrap(), first);
    }
    assert_eq!(store.reads.load(Ordering::SeqCst), 0);
    assert_eq!(platform.gets.load(Ordering::SeqCst), 2);
    service.activate(&c, None, deadline()).await.unwrap();
    let before = platform.gets.load(Ordering::SeqCst);
    service.activate(&a, None, deadline()).await.unwrap();
    assert_eq!(platform.gets.load(Ordering::SeqCst), before);
    service.activate(&b, None, deadline()).await.unwrap();
    assert_eq!(platform.gets.load(Ordering::SeqCst), before + 1);
    tokio::time::advance(Duration::from_secs(11)).await;
    assert_eq!(
        platform.gets.load(Ordering::SeqCst),
        before + 1,
        "idle expiry has no background work"
    );
    assert_eq!(service.activate(&a, None, deadline()).await.unwrap(), first);
    assert_eq!(platform.gets.load(Ordering::SeqCst), before + 2);
}
