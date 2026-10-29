//! Real Redis + fake Platform: no runtime or cluster drain guarantees are exercised.
use adx_activator::{
    reconciliation::{ReconcileSettings, Reconciliation},
    registration::RegistrationConfig,
    Activator,
};
use adx_agent_core::{
    alias::{AliasConfig, VersionWeight},
    discovery::{ranked_endpoints, ActivatorEndpoint},
    environment::*,
    *,
};
use adx_agent_store::{discovery::RedisRegistry, AgentState, RedisRepository};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct Platform(Mutex<BTreeMap<String, EnvironmentObservation>>);
#[async_trait::async_trait]
impl Environment for Platform {
    async fn create(
        &self,
        request: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        let mut records = self.0.lock().unwrap();
        Ok(records
            .entry(request.id.clone())
            .or_insert_with(|| EnvironmentObservation {
                id: request.id.clone(),
                tenant: request.tenant.clone(),
                phase: EnvironmentPhase::Running,
                ready: true,
                runtime_id: Some(request.id.clone()),
                message: None,
            })
            .clone())
    }
    async fn get(
        &self,
        _tenant: &str,
        id: &str,
    ) -> Result<Option<EnvironmentObservation>, EnvironmentError> {
        Ok(self.0.lock().unwrap().get(id).cloned())
    }
    async fn delete(
        &self,
        _tenant: &str,
        id: &str,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        let mut records = self.0.lock().unwrap();
        let record = records.get_mut(id).ok_or(EnvironmentError::NotFound)?;
        record.phase = EnvironmentPhase::Deleted;
        record.ready = false;
        Ok(record.clone())
    }
}
fn settings() -> ReconcileSettings {
    ReconcileSettings {
        concurrency: 2,
        request_timeout_seconds: 5,
        membership_refresh_seconds: 1,
    }
}
fn registration(id: &str) -> RegistrationConfig {
    RegistrationConfig {
        instance_id: id.into(),
        advertise_url: format!("http://{id}:8080"),
        lease_seconds: 6,
        heartbeat_seconds: 1,
    }
}
async fn converged(state: &AgentState, scope: &Scope, version: &str) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let binding = state.binding(scope).await.unwrap().unwrap();
            if binding.current.version == version && binding.secondary.is_none() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("event-driven convergence");
}
async fn fixture() -> (
    RedisRepository,
    RedisRegistry,
    AgentState,
    Arc<Activator>,
    Arc<Platform>,
    AliasConfig,
) {
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").expect("disposable Redis URL");
    let namespace = format!("reconcile-{}", uuid::Uuid::new_v4());
    let store = RedisRepository::connect(&url, &namespace, Duration::from_secs(3))
        .await
        .unwrap();
    let registry = RedisRegistry::new(&url, &namespace, Duration::from_secs(3)).unwrap();
    let state = AgentState::new(Arc::new(store.clone()));
    let platform = Arc::new(Platform::default());
    let activator = Arc::new(Activator::new(state.clone(), platform.clone()));
    for version in ["1", "2"] {
        let template=serde_json::from_value(serde_json::json!({"name":"agent","version":version,"image":format!("agent:{version}"),"isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512}})).unwrap();
        activator
            .publish("tenant", &adx_agent_core::TemplateDraft::from(&template))
            .await
            .unwrap();
    }
    let config = AliasConfig {
        tenant: "tenant".into(),
        template: "agent".into(),
        alias: "production".into(),
        seed: "stable".into(),
        versions: vec![VersionWeight {
            version: "1".into(),
            weight: 1,
        }],
    };
    state.put_alias(&config, None).await.unwrap();
    (store, registry, state, activator, platform, config)
}
async fn select(state: &AgentState, config: &mut AliasConfig, version: &str) {
    let (revision, _) = state
        .alias("tenant", "agent", "production")
        .await
        .unwrap()
        .unwrap();
    config.versions = vec![VersionWeight {
        version: version.into(),
        weight: 1,
    }];
    state.put_alias(config, Some(&revision)).await.unwrap();
}
fn scope(id: &str) -> Scope {
    Scope {
        tenant: "tenant".into(),
        template: "agent".into(),
        template_ref: TemplateRef::Alias("production".into()),
        binding_id: id.into(),
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis with notify-keyspace-events K$g"]
async fn member_removal_transfers_unfinished_bindings_without_an_alias_update() {
    let (store, registry, state, activator, platform, mut config) = fixture().await;
    let members: Vec<_> = ["departing", "survivor"]
        .into_iter()
        .map(|id| ActivatorEndpoint {
            id: id.into(),
            url: format!("http://{id}:8080"),
        })
        .collect();
    let scope = (0..100)
        .map(|i| scope(&i.to_string()))
        .find(|scope| ranked_endpoints(scope, &members)[0].id == "departing")
        .unwrap();
    let original = activator
        .activate(&scope, None, unix_time_millis() + 5000)
        .await
        .unwrap()
        .binding;
    registry
        .renew(&members[0], "ghost", Duration::from_secs(60))
        .await
        .unwrap();
    let survivor = Reconciliation::start(
        activator.clone(),
        store,
        registry.clone(),
        registration("survivor"),
        settings(),
    )
    .await
    .unwrap();
    select(&state, &mut config, "2").await;
    tokio::time::sleep(Duration::from_millis(1300)).await;
    assert_eq!(
        state.binding(&scope).await.unwrap().unwrap().current,
        original.current,
        "non-owner must not prepare the binding"
    );
    assert_eq!(platform.0.lock().unwrap().len(), 1);
    registry.unregister("departing", "ghost").await.unwrap();
    converged(&state, &scope, "2").await;
    let upgraded = state.binding(&scope).await.unwrap().unwrap();
    assert_eq!(upgraded.generation, original.generation);
    assert_eq!(
        platform.0.lock().unwrap()[&original.current.environment_id].phase,
        EnvironmentPhase::Deleted
    );
    survivor.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable Redis with notify-keyspace-events K$g"]
async fn startup_scan_and_later_notifications_converge_and_invalidate_hot_cache() {
    let (store, registry, state, activator, platform, mut config) = fixture().await;
    let scope = scope("workspace");
    let original = activator
        .activate(&scope, None, unix_time_millis() + 5000)
        .await
        .unwrap()
        .binding;
    select(&state, &mut config, "2").await;
    let worker = Reconciliation::start(
        activator.clone(),
        store,
        registry,
        registration("only"),
        settings(),
    )
    .await
    .unwrap();
    converged(&state, &scope, "2").await;
    assert_eq!(
        activator
            .activate(&scope, None, unix_time_millis() + 5000)
            .await
            .unwrap()
            .binding
            .current
            .version,
        "2"
    );
    select(&state, &mut config, "1").await;
    converged(&state, &scope, "1").await;
    let restored = activator
        .activate(&scope, None, unix_time_millis() + 5000)
        .await
        .unwrap()
        .binding;
    assert_eq!(restored.generation, original.generation);
    assert_ne!(
        restored.current.environment_id, original.current.environment_id,
        "retired Env must never be promoted again"
    );
    assert_eq!(platform.0.lock().unwrap().len(), 3);
    worker.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable Redis; disconnects all Pub/Sub clients"]
async fn subscription_reconnect_rescans_changes_missed_while_disconnected() {
    let (store, registry, state, activator, _platform, mut config) = fixture().await;
    let scope = scope("reconnect");
    activator
        .activate(&scope, None, unix_time_millis() + 5000)
        .await
        .unwrap();
    let worker = Reconciliation::start(
        activator.clone(),
        store,
        registry,
        registration("only"),
        settings(),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !activator.ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let mut connection = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let killed: u64 = redis::cmd("CLIENT")
        .arg("KILL")
        .arg("TYPE")
        .arg("PUBSUB")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(killed > 0);
    // Observe the disconnect before writing, so no surviving subscription can deliver this change.
    tokio::time::timeout(Duration::from_millis(800), async {
        while activator.ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("subscription disconnect must disable alias cache");
    select(&state, &mut config, "2").await;
    converged(&state, &scope, "2").await;
    assert!(activator.ready());
    worker.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable Redis with notify-keyspace-events K$g"]
async fn two_live_activators_invalidate_their_caches_on_the_same_binding_switch() {
    let (store, registry, state, first, platform, mut config) = fixture().await;
    let second = Arc::new(Activator::new(state.clone(), platform.clone()));
    let scope = scope("two-live-replicas");
    let deadline = unix_time_millis() + 5000;
    let old = first
        .activate(&scope, None, deadline)
        .await
        .unwrap()
        .binding;
    second.activate(&scope, None, deadline).await.unwrap();
    let one = Reconciliation::start(
        first.clone(),
        store.clone(),
        registry.clone(),
        registration("one"),
        settings(),
    )
    .await
    .unwrap();
    let two = Reconciliation::start(
        second.clone(),
        store,
        registry,
        registration("two"),
        settings(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    select(&state, &mut config, "2").await;
    converged(&state, &scope, "2").await;
    // Delivery is asynchronous; each replica must eventually stop returning its old hot target.
    for activator in [&first, &second] {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let target = activator
                    .activate(&scope, None, unix_time_millis() + 3000)
                    .await
                    .unwrap();
                if target.binding.current.version == "2" {
                    assert_eq!(target.binding.generation, old.generation);
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
    }
    assert_eq!(platform.0.lock().unwrap().len(), 2);
    one.shutdown().await.unwrap();
    two.shutdown().await.unwrap();
}
