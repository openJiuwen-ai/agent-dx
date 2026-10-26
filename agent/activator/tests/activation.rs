use adx_activator::{Activator, Error};
use adx_agent_core::{environment::*, *};
use adx_agent_store::{AgentState, MemoryRepository};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

#[derive(Default)]
struct Platform {
    observations: Mutex<HashMap<(String, String), EnvironmentObservation>>,
    lose_create_response: AtomicBool,
    lose_delete_response: AtomicBool,
    create_entered: tokio::sync::Notify,
    create_gate: Mutex<Option<Arc<tokio::sync::Notify>>>,
}
#[async_trait::async_trait]
impl Environment for Platform {
    async fn create(
        &self,
        r: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        let gate = self.create_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            self.create_entered.notify_one();
            gate.notified().await;
        }
        let observed = self
            .observations
            .lock()
            .unwrap()
            .entry((r.tenant.clone(), r.id.clone()))
            .or_insert_with(|| EnvironmentObservation {
                id: r.id.clone(),
                tenant: r.tenant.clone(),
                phase: EnvironmentPhase::Running,
                ready: true,
                runtime_id: Some("runtime".into()),
                message: None,
            })
            .clone();
        if self.lose_create_response.swap(false, Ordering::SeqCst) {
            return Err(EnvironmentError::OutcomeUnknown("response lost".into()));
        }
        Ok(observed)
    }
    async fn get(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<EnvironmentObservation>, EnvironmentError> {
        Ok(self
            .observations
            .lock()
            .unwrap()
            .get(&(tenant.into(), id.into()))
            .cloned())
    }
    async fn delete(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        if self.lose_delete_response.swap(false, Ordering::SeqCst) {
            return Err(EnvironmentError::OutcomeUnknown(
                "delete response lost".into(),
            ));
        }
        let mut values = self.observations.lock().unwrap();
        let value = values
            .get_mut(&(tenant.into(), id.into()))
            .ok_or(EnvironmentError::NotFound)?;
        value.phase = EnvironmentPhase::Deleted;
        value.ready = false;
        Ok(value.clone())
    }
}
fn template() -> TemplateVersion {
    serde_json::from_value(serde_json::json!({"name":"app","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080}]})).unwrap()
}
fn scope(id: &str) -> Scope {
    Scope {
        tenant: "tenant".into(),
        template: "app".into(),
        version: "1".into(),
        binding_id: id.into(),
    }
}
#[tokio::test]
async fn replicas_and_unknown_outcomes_keep_the_committed_identity() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let a = Activator::new(state.clone(), platform.clone());
    let b = Activator::new(state.clone(), platform.clone());
    a.publish("tenant", &template()).await.unwrap();
    platform.lose_create_response.store(true, Ordering::SeqCst);
    a.activate(
        &scope("e"),
        None,
        adx_agent_core::unix_time_millis() + 60_000,
    )
    .await
    .expect("query and retry within the original activation deadline");
    let original = a.binding(&scope("e")).await.unwrap();
    let e = scope("e");
    let (x, y) = tokio::join!(
        a.activate(&e, None, adx_agent_core::unix_time_millis() + 60_000),
        b.activate(&e, None, adx_agent_core::unix_time_millis() + 60_000)
    );
    assert_eq!(x.unwrap(), y.unwrap());
    assert_eq!(b.binding(&scope("e")).await.unwrap(), original);
    assert_eq!(platform.observations.lock().unwrap().len(), 1);
    platform.lose_delete_response.store(true, Ordering::SeqCst);
    assert!(matches!(
        b.delete_binding(&scope("e")).await,
        Err(Error::OutcomeUnknown(_))
    ));
    assert!(matches!(
        a.activate_with_cache(
            &scope("e"),
            None,
            adx_agent_core::unix_time_millis() + 60_000,
            true,
        )
        .await,
        Err(Error::Conflict(_))
    ));
    a.delete_binding(&scope("e")).await.unwrap();
    let fresh = b
        .activate(
            &scope("e"),
            None,
            adx_agent_core::unix_time_millis() + 60_000,
        )
        .await
        .unwrap()
        .binding;
    assert_ne!(fresh.environment_id, original.environment_id);
    assert_eq!(
        a.activate(
            &scope("e"),
            None,
            adx_agent_core::unix_time_millis() + 60_000
        )
        .await
        .unwrap()
        .binding,
        fresh
    );
}
#[tokio::test]
async fn independent_bindings_and_platform_readiness() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let service = Activator::new(state.clone(), platform.clone());
    service.publish("tenant", &template()).await.unwrap();
    let sa = scope("a");
    let sb = scope("b");
    let (a, b) = tokio::join!(
        service.activate(&sa, None, adx_agent_core::unix_time_millis() + 60_000),
        service.activate(&sb, None, adx_agent_core::unix_time_millis() + 60_000)
    );
    let x = a.unwrap().binding;
    let y = b.unwrap().binding;
    assert_ne!(x.environment_id, y.environment_id);
    platform
        .observations
        .lock()
        .unwrap()
        .get_mut(&(x.scope.tenant.clone(), x.environment_id.clone()))
        .unwrap()
        .ready = false;
    assert!(matches!(
        service
            .activate_with_cache(
                &scope("a"),
                None,
                adx_agent_core::unix_time_millis() + 60_000,
                true,
            )
            .await,
        Err(Error::NotReady(_))
    ));
    assert_eq!(service.binding(&scope("a")).await.unwrap(), x);
    let other = Scope {
        tenant: "other".into(),
        ..scope("a")
    };
    assert!(matches!(
        service
            .activate(&other, None, adx_agent_core::unix_time_millis() + 60_000)
            .await,
        Err(Error::NotFound)
    ));
}

#[tokio::test]
async fn first_traffic_creates_one_binding_across_activators() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let a = Activator::new(state.clone(), platform.clone());
    let b = Activator::new(state.clone(), platform.clone());
    a.publish("tenant", &template()).await.unwrap();
    let scope = scope("traffic");
    let (left, right) = tokio::join!(
        a.activate(&scope, None, adx_agent_core::unix_time_millis() + 60_000),
        b.activate(&scope, None, adx_agent_core::unix_time_millis() + 60_000)
    );
    let first = left.unwrap();
    assert_eq!(first, right.unwrap());
    assert_eq!(platform.observations.lock().unwrap().len(), 1);
    a.delete_binding(&scope).await.unwrap();
    let recreated = b
        .activate_with_cache(
            &scope,
            None,
            adx_agent_core::unix_time_millis() + 60_000,
            true,
        )
        .await
        .unwrap();
    assert_ne!(first.binding.generation, recreated.binding.generation);
}

#[tokio::test]
async fn delayed_retry_cannot_recreate_or_select_another_generation() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let activator = Activator::new(state, platform.clone());
    activator.publish("tenant", &template()).await.unwrap();
    let scope = scope("retry");
    let first = activator
        .activate(&scope, None, adx_agent_core::unix_time_millis() + 60_000)
        .await
        .unwrap();
    activator.delete_binding(&scope).await.unwrap();
    assert!(matches!(
        activator
            .activate(
                &scope,
                Some(&first.binding.generation),
                adx_agent_core::unix_time_millis() + 60_000
            )
            .await,
        Err(Error::Conflict(_))
    ));
    assert!(matches!(
        activator.binding(&scope).await,
        Err(Error::NotFound)
    ));
    let fresh = activator
        .activate(&scope, None, adx_agent_core::unix_time_millis() + 60_000)
        .await
        .unwrap();
    assert!(matches!(
        activator
            .activate(
                &scope,
                Some(&first.binding.generation),
                adx_agent_core::unix_time_millis() + 60_000
            )
            .await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(
        activator
            .activate(
                &scope,
                Some(&fresh.binding.generation),
                adx_agent_core::unix_time_millis() + 60_000
            )
            .await
            .unwrap(),
        fresh
    );
    assert_eq!(platform.observations.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn deleting_absent_environment_removes_binding_and_allows_new_generation() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let service = Activator::new(state.clone(), platform.clone());
    service.publish("tenant", &template()).await.unwrap();
    let scope = scope("missing");
    let old = service
        .activate(&scope, None, adx_agent_core::unix_time_millis() + 60_000)
        .await
        .unwrap()
        .binding;
    platform
        .observations
        .lock()
        .unwrap()
        .remove(&(scope.tenant.clone(), old.environment_id.clone()));
    service.delete_binding(&scope).await.unwrap();
    assert!(matches!(
        service.binding(&scope).await,
        Err(Error::NotFound)
    ));
    let fresh = service
        .activate(&scope, None, adx_agent_core::unix_time_millis() + 60_000)
        .await
        .unwrap()
        .binding;
    assert_ne!(fresh.generation, old.generation);
    assert_ne!(fresh.environment_id, old.environment_id);
}

#[tokio::test]
async fn dormant_bindings_resolve_after_cache_loss_without_recreating_or_waking() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let service = Activator::new(state.clone(), platform.clone());
    service.publish("tenant", &template()).await.unwrap();
    let scope = scope("sleep");
    let original = service
        .activate(&scope, None, unix_time_millis() + 60_000)
        .await
        .unwrap();
    for phase in ["pausing", "paused", "resuming"] {
        let observation: EnvironmentObservation = serde_json::from_value(serde_json::json!({
            "id":original.binding.environment_id, "tenant":"tenant", "phase":phase,
            "ready":false, "runtime_id":null, "message":null
        }))
        .unwrap();
        platform.observations.lock().unwrap().insert(
            ("tenant".into(), original.binding.environment_id.clone()),
            observation.clone(),
        );
        let cold = Activator::new(state.clone(), platform.clone());
        let target = cold
            .activate(
                &scope,
                Some(&original.binding.generation),
                unix_time_millis() + 60_000,
            )
            .await
            .unwrap();
        assert_eq!(target, original);
        let rows = platform.observations.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows.values().next().unwrap(), &observation);
    }
}

#[tokio::test]
async fn late_activation_after_binding_deletion_cleans_only_the_old_environment() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let a = Arc::new(Activator::new(state.clone(), platform.clone()));
    let b = Activator::new(state.clone(), platform.clone());
    a.publish("tenant", &template()).await.unwrap();
    let gate = Arc::new(tokio::sync::Notify::new());
    *platform.create_gate.lock().unwrap() = Some(gate.clone());
    let pending = {
        let a = a.clone();
        tokio::spawn(async move {
            a.activate(&scope("racing"), None, unix_time_millis() + 60_000)
                .await
        })
    };
    platform.create_entered.notified().await;
    let old = b.binding(&scope("racing")).await.unwrap();
    b.delete_binding(&scope("racing")).await.unwrap();
    let fresh = b
        .activate(&scope("racing"), None, unix_time_millis() + 60_000)
        .await
        .unwrap()
        .binding;
    gate.notify_one();
    assert!(pending.await.unwrap().is_err());
    let observations = platform.observations.lock().unwrap();
    assert_eq!(
        observations[&("tenant".into(), old.environment_id)].phase,
        EnvironmentPhase::Deleted
    );
    assert_eq!(
        observations[&("tenant".into(), fresh.environment_id)].phase,
        EnvironmentPhase::Running
    );
}
