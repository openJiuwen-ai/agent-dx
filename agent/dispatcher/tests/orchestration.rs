#[path = "../../tests/pool_fixture.rs"]
mod pool_fixture;
use adx_agent_core::{sandbox::*, *};
use adx_agent_store::{AgentState, MemoryRepository, RedisRepository, Repository};
use adx_dispatcher::{Config, Dispatcher, ResolveRequest};
use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{Mutex, Notify};

#[derive(Default)]
struct Backend {
    instances: Mutex<BTreeMap<(String, String), SandboxObservation>>,
    creates: AtomicUsize,
    gets: AtomicUsize,
    fail_response: AtomicBool,
    pending: AtomicBool,
    fail_delete: AtomicBool,
    stall_get: AtomicBool,
    deletes: AtomicUsize,
    pause: AtomicBool,
    entered: Notify,
    resume: Notify,
}
#[async_trait]
impl Sandbox for Backend {
    async fn create(&self, r: &CreateSandbox) -> Result<SandboxObservation, SandboxError> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        if self.pause.load(Ordering::SeqCst) {
            self.entered.notify_one();
            self.resume.notified().await;
        }
        let result = self
            .instances
            .lock()
            .await
            .entry((r.tenant.clone(), r.id.clone()))
            .or_insert_with(|| SandboxObservation {
                id: r.id.clone(),
                tenant: r.tenant.clone(),
                phase: SandboxPhase::Running,
                ready: !self.pending.load(Ordering::SeqCst),
                runtime_id: Some("generation-1".into()),
                message: None,
            })
            .clone();
        if self.fail_response.swap(false, Ordering::SeqCst) {
            return Err(SandboxError::OutcomeUnknown(
                "injected lost response".into(),
            ));
        }
        Ok(result)
    }
    async fn get(&self, t: &str, id: &str) -> Result<Option<SandboxObservation>, SandboxError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        if self.stall_get.load(Ordering::SeqCst) {
            self.entered.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(self
            .instances
            .lock()
            .await
            .get(&(t.into(), id.into()))
            .cloned())
    }
    async fn delete(&self, t: &str, id: &str) -> Result<SandboxObservation, SandboxError> {
        self.deletes.fetch_add(1, Ordering::SeqCst);
        if self.fail_delete.load(Ordering::SeqCst) {
            return Err(SandboxError::OutcomeUnknown(
                "injected delete failure".into(),
            ));
        }
        // This mock deliberately supplies backend fencing, not merely a successful local delete.
        let result = SandboxObservation {
            id: id.into(),
            tenant: t.into(),
            phase: SandboxPhase::Deleted,
            ready: false,
            runtime_id: None,
            message: None,
        };
        self.instances
            .lock()
            .await
            .insert((t.into(), id.into()), result.clone());
        Ok(result)
    }
}
fn dispatcher(state: AgentState, backend: Arc<Backend>) -> Arc<Dispatcher> {
    Arc::new(
        Dispatcher::new(
            state,
            backend,
            uuid::Uuid::new_v4().to_string(),
            Config {
                create_timeout: Duration::from_secs(2),
                poll_interval: Duration::from_millis(1),
                ..Config::default()
            },
        )
        .unwrap(),
    )
}
async fn setup(store: Arc<dyn Repository>) -> (AgentState, Scope) {
    let state = AgentState::new(store);
    let template=serde_json::from_value(serde_json::json!({"name":"test","version":"1","image":"app:1","isolation_runtime":"runc","entrypoint":["/app/start"],"resources":{"cpu_millis":1000,"memory_mib":256}})).unwrap();
    state.publish("tenant", &template).await.unwrap();
    let scope = Scope {
        tenant: "tenant".into(),
        template: "test".into(),
        version: "1".into(),
        session_id: "ctx".into(),
    };
    state.create_session(scope.clone()).await.unwrap();
    (state, scope)
}
async fn competing_clients(a: Arc<dyn Repository>, b: Arc<dyn Repository>) {
    let (state, scope) = setup(a).await;
    let backend = Arc::new(Backend::default());
    let first = dispatcher(state.clone(), backend.clone());
    let second = dispatcher(AgentState::new(b), backend.clone());
    let request = ResolveRequest {
        bypass_cache: false,
        scope: scope.clone(),
        affinity_key: Some("affinity".into()),
    };
    let (a, b) = tokio::join!(first.resolve(&request), second.resolve(&request));
    let target = a.unwrap();
    assert_eq!(target, b.unwrap());
    assert_eq!(state.pool(&scope).await.unwrap().len(), 1);
    assert_eq!(backend.instances.lock().await.len(), 1);
    let replacement = dispatcher(state.clone(), backend.clone());
    assert_eq!(target, replacement.resolve(&request).await.unwrap());
    replacement
        .release_instance(&scope, &target.instance_id)
        .await
        .unwrap();
    // Stale cache is allowed until forwarding fails; bypass converges without explicit affinity release.
    assert_eq!(first.resolve(&request).await.unwrap(), target);
    let fresh_request = ResolveRequest {
        bypass_cache: true,
        ..request.clone()
    };
    let next = first.resolve(&fresh_request).await.unwrap();
    assert_ne!(next.instance_id, target.instance_id);
    assert_eq!(second.resolve(&fresh_request).await.unwrap(), next);
    second.release(&scope).await.unwrap();
    assert!(state.session(&scope).await.unwrap().is_none());
}
#[tokio::test]
async fn two_dispatchers_cold_start_and_restart_keep_one_binding() {
    let store = Arc::new(MemoryRepository::default());
    competing_clients(store.clone(), store).await;
}
#[tokio::test]
#[ignore = "requires ADX_AGENT_TEST_REDIS_URL for disposable Redis"]
async fn independent_redis_dispatchers_converge() {
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let ns = format!("dispatch-{}", uuid::Uuid::new_v4());
    let a = Arc::new(
        RedisRepository::connect(&url, &ns, Duration::from_secs(3))
            .await
            .unwrap(),
    );
    let b = Arc::new(
        RedisRepository::connect(&url, &ns, Duration::from_secs(3))
            .await
            .unwrap(),
    );
    competing_clients(a, b).await;
}
#[tokio::test]
async fn lost_create_response_recovers_original_identity_without_recreate() {
    let (state, scope) = setup(Arc::new(MemoryRepository::default())).await;
    let backend = Arc::new(Backend::default());
    backend.fail_response.store(true, Ordering::SeqCst);
    let first = dispatcher(state.clone(), backend.clone());
    assert!(first
        .resolve(&ResolveRequest {
            scope: scope.clone(),
            affinity_key: None,
            bypass_cache: false
        })
        .await
        .is_err());
    let reserved = state.pool(&scope).await.unwrap().remove(0);
    assert_eq!(reserved.phase, InstancePhase::Creating);
    drop(first);
    let replacement = dispatcher(state.clone(), backend.clone());
    replacement.recover_page(0).await.unwrap();
    let target = replacement
        .resolve(&ResolveRequest {
            bypass_cache: false,
            scope,
            affinity_key: None,
        })
        .await
        .unwrap();
    assert_eq!(target.instance_id, reserved.id);
    assert_eq!(backend.creates.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn release_fences_a_late_create_from_another_dispatcher() {
    let (state, scope) = setup(Arc::new(MemoryRepository::default())).await;
    let backend = Arc::new(Backend::default());
    backend.pause.store(true, Ordering::SeqCst);
    let a = dispatcher(state.clone(), backend.clone());
    let b = dispatcher(state.clone(), backend.clone());
    let creating = tokio::spawn({
        let scope = scope.clone();
        async move {
            a.resolve(&ResolveRequest {
                scope,
                affinity_key: None,
                bypass_cache: false,
            })
            .await
        }
    });
    tokio::time::timeout(Duration::from_secs(2), backend.entered.notified())
        .await
        .unwrap();
    b.release(&scope).await.unwrap();
    backend.resume.notify_one();
    assert!(creating.await.unwrap().is_err()); // Session has already been deleted.
    let instances = backend.instances.lock().await;
    assert_eq!(instances.len(), 1);
    assert!(instances.values().all(|i| i.phase == SandboxPhase::Deleted));
    assert!(state.session(&scope).await.unwrap().is_none());
}
#[tokio::test]
async fn substrate_health_changes_do_not_probe_or_change_ready_instances_and_affinity() {
    let (state, scope) = setup(Arc::new(MemoryRepository::default())).await;
    let backend = Arc::new(Backend::default());
    let first = dispatcher(state.clone(), backend.clone());
    let second = dispatcher(state.clone(), backend.clone());
    let request = ResolveRequest {
        bypass_cache: false,
        scope: scope.clone(),
        affinity_key: Some("sticky".into()),
    };
    let target = first.resolve(&request).await.unwrap();
    let instance = state
        .instance("tenant", &target.instance_id)
        .await
        .unwrap()
        .unwrap();
    let gets = backend.gets.load(Ordering::SeqCst);
    for phase in [
        SandboxPhase::Failed,
        SandboxPhase::Deleted,
        SandboxPhase::Running,
    ] {
        {
            let mut instances = backend.instances.lock().await;
            let observed = instances
                .get_mut(&("tenant".into(), target.sandbox_id.clone()))
                .unwrap();
            observed.phase = phase;
            observed.ready = false;
            observed.runtime_id = Some("new-runtime".into());
        }
        tokio::try_join!(first.recover_page(0), second.recover_page(0)).unwrap();
        second.reconcile(&instance).await.unwrap();
        assert_eq!(
            state
                .instance("tenant", &target.instance_id)
                .await
                .unwrap()
                .unwrap(),
            instance
        );
        assert_eq!(
            second
                .resolve(&ResolveRequest {
                    bypass_cache: true,
                    ..request.clone()
                })
                .await
                .unwrap(),
            target
        );
    }
    assert_eq!(backend.gets.load(Ordering::SeqCst), gets);
    assert_eq!(backend.creates.load(Ordering::SeqCst), 1);
    // Session deletion still cleans stable instances, even before individual delete intents exist.
    state.release_session(&scope).await.unwrap();
    second.recover_sessions_page(0).await.unwrap();
    assert!(state.session(&scope).await.unwrap().is_none());
    assert_eq!(
        state
            .instance("tenant", &target.instance_id)
            .await
            .unwrap()
            .unwrap()
            .phase,
        InstancePhase::Deleted
    );
}
#[tokio::test]
async fn pool_round_robin_selects_ready_instances() {
    let repo = Arc::new(MemoryRepository::default());
    let (state, scope) = setup(repo.clone()).await;
    let backend = Arc::new(Backend::default());
    let dispatcher = dispatcher(state.clone(), backend);
    // Populate test state to exercise selection over an existing multi-instance pool.
    for index in 0..3 {
        let id = format!("fixture-{index}");
        pool_fixture::insert_instance(repo.as_ref(), &scope, &id, InstancePhase::Ready).await;
    }
    let request = ResolveRequest {
        bypass_cache: false,
        scope: scope.clone(),
        affinity_key: None,
    };
    let mut counts = BTreeMap::new();
    for _ in 0..30 {
        *counts
            .entry(dispatcher.resolve(&request).await.unwrap().instance_id)
            .or_insert(0) += 1;
    }
    assert_eq!(counts.len(), 3);
    assert!(counts.values().all(|n| *n == 10));
}

#[tokio::test]
async fn startup_readiness_is_checked_and_stable_states_skip_probes() {
    let repo = Arc::new(MemoryRepository::default());
    let (state, scope) = setup(repo.clone()).await;
    let backend = Arc::new(Backend::default());
    let node = dispatcher(state.clone(), backend.clone());
    for (id, phase) in [
        ("starting", SandboxPhase::Running),
        ("failed", SandboxPhase::Failed),
    ] {
        pool_fixture::insert_instance(repo.as_ref(), &scope, id, InstancePhase::Creating).await;
        backend.instances.lock().await.insert(
            ("tenant".into(), id.into()),
            SandboxObservation {
                id: id.into(),
                tenant: "tenant".into(),
                phase,
                ready: false,
                runtime_id: None,
                message: None,
            },
        );
    }
    let stale = state.instance("tenant", "failed").await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    node.recover_page(0).await.unwrap();
    assert_eq!(
        state
            .instance("tenant", "starting")
            .await
            .unwrap()
            .unwrap()
            .phase,
        InstancePhase::Creating
    );
    assert_eq!(
        state
            .instance("tenant", "failed")
            .await
            .unwrap()
            .unwrap()
            .phase,
        InstancePhase::Failed
    );
    for observed in backend.instances.lock().await.values_mut() {
        observed.phase = SandboxPhase::Running;
        observed.ready = true;
    }
    tokio::time::sleep(Duration::from_millis(2)).await;
    node.recover_page(0).await.unwrap();
    assert_eq!(
        state
            .instance("tenant", "starting")
            .await
            .unwrap()
            .unwrap()
            .phase,
        InstancePhase::Ready
    );
    let gets = backend.gets.load(Ordering::SeqCst);
    node.recover_page(0).await.unwrap();
    node.reconcile(&stale).await.unwrap();
    assert_eq!(backend.gets.load(Ordering::SeqCst), gets);
    assert_eq!(
        state
            .instance("tenant", "failed")
            .await
            .unwrap()
            .unwrap()
            .phase,
        InstancePhase::Failed
    );
    assert_eq!(
        state
            .instance("tenant", "starting")
            .await
            .unwrap()
            .unwrap()
            .phase,
        InstancePhase::Ready
    );
    node.release_instance(&scope, "failed").await.unwrap();
    assert_eq!(
        state
            .instance("tenant", "failed")
            .await
            .unwrap()
            .unwrap()
            .phase,
        InstancePhase::Deleted
    );
}

#[derive(Default)]
struct ObservedRepository {
    inner: MemoryRepository,
    reads: AtomicUsize,
    writes: AtomicUsize,
    unavailable: AtomicBool,
    pause_affinity_read: AtomicBool,
    pause_instance_read: AtomicBool,
    read_paused: Notify,
    resume_read: Notify,
}
#[async_trait]
impl Repository for ObservedRepository {
    async fn get(
        &self,
        key: &adx_agent_store::Key,
    ) -> adx_agent_store::Result<Option<adx_agent_store::Record>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(adx_agent_store::Error::Unavailable(
                "injected outage".into(),
            ));
        }
        let record = self.inner.get(key).await?;
        let pause = (key.as_str().starts_with("affinity:")
            && self.pause_affinity_read.swap(false, Ordering::SeqCst))
            || (key.as_str().starts_with("instance:")
                && self.pause_instance_read.swap(false, Ordering::SeqCst));
        if pause {
            self.read_paused.notify_one();
            self.resume_read.notified().await;
        }
        Ok(record)
    }
    async fn commit(&self, tx: &adx_agent_store::Transaction) -> adx_agent_store::Result<bool> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(adx_agent_store::Error::Unavailable(
                "injected outage".into(),
            ));
        }
        self.inner.commit(tx).await
    }
    async fn scan(
        &self,
        kind: &str,
        cursor: u64,
        count: u32,
    ) -> adx_agent_store::Result<adx_agent_store::Page> {
        self.inner.scan(kind, cursor, count).await
    }
}
#[tokio::test]
async fn warm_cache_avoids_storage_and_bypass_never_falls_back() {
    let repo = Arc::new(ObservedRepository::default());
    let (state, scope) = setup(repo.clone()).await;
    let backend = Arc::new(Backend::default());
    let node = dispatcher(state.clone(), backend.clone());
    let request = ResolveRequest {
        scope: scope.clone(),
        affinity_key: Some("key".into()),
        bypass_cache: false,
    };
    let first = node.resolve(&request).await.unwrap();
    let plain = ResolveRequest {
        affinity_key: None,
        ..request.clone()
    };
    repo.reads.store(0, Ordering::SeqCst);
    repo.writes.store(0, Ordering::SeqCst);
    for _ in 0..10 {
        assert_eq!(node.resolve(&request).await.unwrap(), first);
        assert_eq!(node.resolve(&plain).await.unwrap(), first);
    }
    assert_eq!(repo.reads.load(Ordering::SeqCst), 0);
    assert_eq!(repo.writes.load(Ordering::SeqCst), 0);
    repo.unavailable.store(true, Ordering::SeqCst);
    assert_eq!(node.resolve(&request).await.unwrap(), first);
    assert_eq!(node.resolve(&plain).await.unwrap(), first);
    assert!(node
        .resolve(&ResolveRequest {
            affinity_key: Some("new".into()),
            ..request.clone()
        })
        .await
        .is_err());
    assert!(node
        .resolve(&ResolveRequest {
            bypass_cache: true,
            ..request.clone()
        })
        .await
        .is_err());
    assert!(node.resolve(&request).await.is_err()); // failed bypass removed the stale cache
    repo.unavailable.store(false, Ordering::SeqCst);
    assert_eq!(node.resolve(&request).await.unwrap(), first);
    // A second node deletes and recreates the public Session. The old cache can serve its
    // former target, but cannot write a binding into the new lifecycle.
    let other = dispatcher(state.clone(), backend);
    other.release(&scope).await.unwrap();
    state.create_session(scope.clone()).await.unwrap();
    assert_eq!(node.resolve(&request).await.unwrap(), first);
    assert!(node
        .resolve(&ResolveRequest {
            affinity_key: Some("late".into()),
            ..request.clone()
        })
        .await
        .is_err());
    let fresh = node
        .resolve(&ResolveRequest {
            bypass_cache: true,
            ..request
        })
        .await
        .unwrap();
    assert_ne!(fresh.session_generation, first.session_generation);
    assert_ne!(fresh.instance_id, first.instance_id);
    assert!(state.affinity(&scope, "late").await.unwrap().is_none());
}

#[tokio::test]
async fn affinity_capacity_evicts_one_binding_instead_of_flushing_warm_entries() {
    let repo = Arc::new(ObservedRepository::default());
    let (state, scope) = setup(repo.clone()).await;
    let node = Dispatcher::new(
        state,
        Arc::new(Backend::default()),
        uuid::Uuid::new_v4().to_string(),
        Config {
            max_cached_affinities_per_session: 2,
            ..Config::default()
        },
    )
    .unwrap();
    for key in ["a", "b", "c"] {
        node.resolve(&ResolveRequest {
            scope: scope.clone(),
            affinity_key: Some(key.into()),
            bypass_cache: false,
        })
        .await
        .unwrap();
    }
    repo.reads.store(0, Ordering::SeqCst);
    repo.writes.store(0, Ordering::SeqCst);
    for key in ["b", "c"] {
        node.resolve(&ResolveRequest {
            scope: scope.clone(),
            affinity_key: Some(key.into()),
            bypass_cache: false,
        })
        .await
        .unwrap();
    }
    assert_eq!(repo.reads.load(Ordering::SeqCst), 0);
    assert_eq!(repo.writes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn creation_timeout_persists_delete_intent_and_replacement_finishes_cleanup() {
    let (state, scope) = setup(Arc::new(MemoryRepository::default())).await;
    let backend = Arc::new(Backend::default());
    backend.pending.store(true, Ordering::SeqCst);
    backend.fail_delete.store(true, Ordering::SeqCst);
    let node = Dispatcher::new(
        state.clone(),
        backend.clone(),
        uuid::Uuid::new_v4().to_string(),
        Config {
            create_timeout: Duration::from_millis(40),
            poll_interval: Duration::from_millis(5),
            max_poll_interval: Duration::from_millis(20),
            ..Config::default()
        },
    )
    .unwrap();
    let request = ResolveRequest {
        scope: scope.clone(),
        affinity_key: None,
        bypass_cache: false,
    };
    assert!(node.resolve(&request).await.is_err());
    node.recover_page(0).await.unwrap();
    let expired = state.pool(&scope).await.unwrap().remove(0);
    assert_eq!(expired.phase, InstancePhase::Deleting);
    assert_eq!(expired.desired, DesiredState::Deleted);
    assert_eq!(
        expired.status_message.as_deref(),
        Some(limits::CREATE_TIMEOUT_MESSAGE)
    );
    assert_eq!(backend.creates.load(Ordering::SeqCst), 1);
    let delete_attempts = backend.deletes.load(Ordering::SeqCst);
    assert!(delete_attempts >= 1);
    assert!(state.mark_ready("tenant", &expired.id).await.is_err());
    drop(node);
    backend.fail_delete.store(false, Ordering::SeqCst);
    // Replacement uses the stored deadline, regardless of its longer configuration.
    let replacement = dispatcher(state.clone(), backend.clone());
    let queries = backend.gets.load(Ordering::SeqCst);
    replacement.recover_page(0).await.unwrap();
    let deleted = state
        .instance("tenant", &expired.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(deleted.phase, InstancePhase::Deleted);
    assert_eq!(deleted.create_deadline_ms, expired.create_deadline_ms);
    assert!(state.pool(&scope).await.unwrap().is_empty());
    replacement.recover_page(0).await.unwrap();
    assert_eq!(backend.gets.load(Ordering::SeqCst), queries);
    assert_eq!(backend.creates.load(Ordering::SeqCst), 1);
    assert_eq!(backend.deletes.load(Ordering::SeqCst), delete_attempts + 1);
}

#[tokio::test]
async fn replacement_expires_original_deadline_without_querying_or_recreating() {
    let (state, scope) = setup(Arc::new(MemoryRepository::default())).await;
    let session = state.session(&scope).await.unwrap().unwrap();
    state
        .reserve_cold_start_in_session(
            &scope,
            &session.generation,
            "expired",
            "expired",
            unix_time_millis() + 1,
        )
        .await
        .unwrap();
    let old = state.instance("tenant", "expired").await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(3)).await;
    let backend = Arc::new(Backend::default());
    let node = dispatcher(state.clone(), backend.clone());
    node.recover_page(0).await.unwrap();
    let expired = state.instance("tenant", "expired").await.unwrap().unwrap();
    assert_eq!(expired.phase, InstancePhase::Deleted);
    assert_eq!(expired.create_deadline_ms, old.create_deadline_ms);
    assert_eq!(backend.gets.load(Ordering::SeqCst), 0);
    assert_eq!(backend.creates.load(Ordering::SeqCst), 0);
    assert_eq!(backend.deletes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn creation_deadline_cancels_stalled_observation_and_deletes_without_refilling() {
    let (state, scope) = setup(Arc::new(MemoryRepository::default())).await;
    let backend = Arc::new(Backend::default());
    backend.stall_get.store(true, Ordering::SeqCst);
    let node = Dispatcher::new(
        state.clone(),
        backend.clone(),
        uuid::Uuid::new_v4().to_string(),
        Config {
            create_timeout: Duration::from_millis(30),
            ..Config::default()
        },
    )
    .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        node.resolve(&ResolveRequest {
            scope: scope.clone(),
            affinity_key: None,
            bypass_cache: false,
        }),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    node.recover_page(0).await.unwrap();
    assert_eq!(backend.gets.load(Ordering::SeqCst), 1);
    assert_eq!(backend.creates.load(Ordering::SeqCst), 0);
    assert_eq!(backend.deletes.load(Ordering::SeqCst), 1);
    assert!(state.pool(&scope).await.unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn creation_queries_back_off_and_recovery_shares_the_gate() {
    let (state, scope) = setup(Arc::new(MemoryRepository::default())).await;
    state
        .reserve_cold_start(&scope, "pending", "pending")
        .await
        .unwrap();
    let instance = state.instance("tenant", "pending").await.unwrap().unwrap();
    let backend = Arc::new(Backend::default());
    backend.pending.store(true, Ordering::SeqCst);
    let node = Dispatcher::new(
        state.clone(),
        backend.clone(),
        uuid::Uuid::new_v4().to_string(),
        Config::default(),
    )
    .unwrap();
    node.reconcile(&instance).await.unwrap();
    assert_eq!(backend.gets.load(Ordering::SeqCst), 1);
    assert_eq!(backend.creates.load(Ordering::SeqCst), 1);
    // Early request retries and repeated recovery scans cannot trigger extra Sandbox queries.
    for delay in [500u64, 1000, 2000, 4000, 5000, 5000] {
        let before = backend.gets.load(Ordering::SeqCst);
        node.recover_page(0).await.unwrap();
        node.reconcile(&instance).await.unwrap();
        assert_eq!(backend.gets.load(Ordering::SeqCst), before);
        tokio::time::advance(Duration::from_millis(delay - 1)).await;
        node.recover_page(0).await.unwrap();
        assert_eq!(backend.gets.load(Ordering::SeqCst), before);
        tokio::time::advance(Duration::from_millis(1)).await;
        node.reconcile(&instance).await.unwrap();
        assert_eq!(backend.gets.load(Ordering::SeqCst), before + 1);
    }
    assert_eq!(backend.creates.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn late_ready_is_fenced_but_already_ready_is_not_expired() {
    let (state, scope) = setup(Arc::new(MemoryRepository::default())).await;
    let session = state.session(&scope).await.unwrap().unwrap();
    state
        .reserve_cold_start_in_session(
            &scope,
            &session.generation,
            "late",
            "late",
            unix_time_millis() + 1,
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(3)).await;
    assert!(state.mark_ready("tenant", "late").await.is_err());
    let late = state.instance("tenant", "late").await.unwrap().unwrap();
    assert_eq!(late.phase, InstancePhase::Deleting);
    assert_eq!(
        late.status_message.as_deref(),
        Some(limits::CREATE_TIMEOUT_MESSAGE)
    );
    state.confirm_deleted("tenant", "late").await.unwrap();
    state
        .reserve_cold_start_in_session(
            &scope,
            &session.generation,
            "ready",
            "ready",
            unix_time_millis() + 100,
        )
        .await
        .unwrap();
    state.mark_ready("tenant", "ready").await.unwrap();
    tokio::time::sleep(Duration::from_millis(110)).await;
    assert_eq!(
        state
            .expire_creation("tenant", "ready")
            .await
            .unwrap()
            .phase,
        InstancePhase::Ready
    );
}

#[tokio::test(start_paused = true)]
async fn resolve_waits_past_thirty_seconds_with_the_shared_creation_budget() {
    let (state, scope) = setup(Arc::new(MemoryRepository::default())).await;
    let backend = Arc::new(Backend::default());
    backend.pause.store(true, Ordering::SeqCst);
    let node = Arc::new(
        Dispatcher::new(
            state.clone(),
            backend.clone(),
            uuid::Uuid::new_v4().to_string(),
            Config::default(),
        )
        .unwrap(),
    );
    let started_ms = unix_time_millis();
    let waiting = tokio::spawn({
        let node = node.clone();
        let scope = scope.clone();
        async move {
            node.resolve(&ResolveRequest {
                scope,
                affinity_key: None,
                bypass_cache: false,
            })
            .await
        }
    });
    backend.entered.notified().await;
    let pending = state.pool(&scope).await.unwrap().remove(0);
    assert!(pending.create_deadline_ms >= started_ms + 60_000);
    tokio::time::advance(Duration::from_secs(45)).await;
    assert!(
        !waiting.is_finished(),
        "Resolve must not have an independent 30-second timeout"
    );
    backend.resume.notify_one();
    let target = waiting.await.unwrap().unwrap();
    assert_eq!(target.instance_id, pending.id);
    assert_eq!(backend.creates.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn slow_affinity_lookup_does_not_block_warm_selection_in_the_same_session() {
    let repo = Arc::new(ObservedRepository::default());
    let (state, scope) = setup(repo.clone()).await;
    let node = dispatcher(state, Arc::new(Backend::default()));
    let warm = ResolveRequest {
        scope: scope.clone(),
        affinity_key: Some("warm".into()),
        bypass_cache: false,
    };
    let target = node.resolve(&warm).await.unwrap();
    repo.pause_affinity_read.store(true, Ordering::SeqCst);
    let slow = tokio::spawn({
        let node = node.clone();
        async move {
            node.resolve(&ResolveRequest {
                scope,
                affinity_key: Some("slow".into()),
                bypass_cache: false,
            })
            .await
        }
    });
    repo.read_paused.notified().await;
    let reads = repo.reads.load(Ordering::SeqCst);
    let selected = tokio::time::timeout(Duration::from_millis(100), node.resolve(&warm))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected, target);
    assert_eq!(repo.reads.load(Ordering::SeqCst), reads);
    repo.resume_read.notify_one();
    assert_eq!(slow.await.unwrap().unwrap(), target);
}

#[tokio::test]
async fn slow_sandbox_creation_does_not_hold_the_session_cache_lock() {
    let (state, scope) = setup(Arc::new(MemoryRepository::default())).await;
    let backend = Arc::new(Backend::default());
    backend.pause.store(true, Ordering::SeqCst);
    let node = dispatcher(state.clone(), backend.clone());
    let request = ResolveRequest {
        scope: scope.clone(),
        affinity_key: None,
        bypass_cache: false,
    };
    let slow = tokio::spawn({
        let node = node.clone();
        let request = request.clone();
        async move { node.resolve(&request).await }
    });
    backend.entered.notified().await;
    let pending = state.pool(&scope).await.unwrap().remove(0);
    // Another observer has confirmed Ready while this create response is still delayed.
    state.mark_ready("tenant", &pending.id).await.unwrap();
    let selected = tokio::time::timeout(
        Duration::from_millis(100),
        node.resolve(&ResolveRequest {
            bypass_cache: true,
            ..request
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(selected.instance_id, pending.id);
    backend.resume.notify_one();
    assert_eq!(slow.await.unwrap().unwrap(), selected);
    assert_eq!(backend.creates.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn delayed_affinity_read_cannot_repopulate_cache_after_bypass() {
    let repo = Arc::new(ObservedRepository::default());
    let (state, scope) = setup(repo.clone()).await;
    let node = dispatcher(state.clone(), Arc::new(Backend::default()));
    let plain = ResolveRequest {
        scope: scope.clone(),
        affinity_key: None,
        bypass_cache: false,
    };
    let old = node.resolve(&plain).await.unwrap();
    state
        .bind(&scope, "sticky", &old.instance_id)
        .await
        .unwrap();
    let request = ResolveRequest {
        affinity_key: Some("sticky".into()),
        ..plain
    };
    repo.pause_instance_read.store(true, Ordering::SeqCst);
    let slow = tokio::spawn({
        let node = node.clone();
        let request = request.clone();
        async move { node.resolve(&request).await }
    });
    repo.read_paused.notified().await; // The old Ready record has been read, but not returned.
    state
        .begin_delete("tenant", &old.instance_id)
        .await
        .unwrap();
    state
        .confirm_deleted("tenant", &old.instance_id)
        .await
        .unwrap();
    state
        .reserve_cold_start(&scope, "replacement", "replacement")
        .await
        .unwrap();
    state.mark_ready("tenant", "replacement").await.unwrap();
    let current = tokio::time::timeout(
        Duration::from_millis(100),
        node.resolve(&ResolveRequest {
            bypass_cache: true,
            ..request.clone()
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(current.instance_id, "replacement");
    repo.resume_read.notify_one();
    assert_eq!(slow.await.unwrap().unwrap(), current);
    repo.unavailable.store(true, Ordering::SeqCst);
    assert_eq!(node.resolve(&request).await.unwrap(), current);
}

#[tokio::test]
async fn caller_deadline_limits_creation_and_expired_requests_do_not_start_it() {
    let (state, scope) = setup(Arc::new(MemoryRepository::default())).await;
    let backend = Arc::new(Backend::default());
    backend.stall_get.store(true, Ordering::SeqCst);
    let node = dispatcher(state.clone(), backend.clone());
    let request = ResolveRequest {
        scope: scope.clone(),
        affinity_key: None,
        bypass_cache: false,
    };
    assert!(matches!(
        node.resolve_with_deadline(&request, Some(unix_time_millis() - 1))
            .await,
        Err(adx_dispatcher::Error::NotReady(_))
    ));
    assert!(state.pool(&scope).await.unwrap().is_empty());
    let deadline = unix_time_millis() + 30;
    assert!(node
        .resolve_with_deadline(&request, Some(deadline))
        .await
        .is_err());
    let (_, instances) = state.scan_instances(0, 100).await.unwrap();
    assert_eq!(instances.len(), 1);
    assert_eq!(instances[0].create_deadline_ms, deadline);
    assert_eq!(instances[0].desired, DesiredState::Deleted);
}
