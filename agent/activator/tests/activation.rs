use adx_activator::{Activator, Error};
use adx_agent_core::{environment::*, *};
use adx_agent_store::{AgentState, MemoryRepository};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Default)]
struct Platform {
    observations: Mutex<HashMap<(String, String), EnvironmentObservation>>,
    lose_create_response: AtomicBool,
    lose_delete_response: AtomicBool,
    pause_blocks: AtomicBool,
    pause_count: std::sync::atomic::AtomicU32,
    // When set, `get` for this Environment id sleeps `delay` before replying. Used to let the
    // reconciliation deadline expire between activate_environment and the checkpoint decision.
    slow_get: Mutex<Option<(String, Duration)>>,
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
                resume_pending: false,
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
        let slow = self.slow_get.lock().unwrap().clone();
        if let Some((slow_id, delay)) = slow {
            if id == slow_id {
                tokio::time::sleep(delay).await;
            }
        }
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
    async fn pause(
        &self,
        tenant: &str,
        id: &str,
        _timeout_seconds: u64,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        self.pause_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.pause_blocks.load(Ordering::SeqCst) {
            // Capture is in flight and never completes within this step; the caller's
            // step budget drops the call so the next pass re-queries the same ID.
            std::future::pending::<()>().await;
        }
        let mut values = self.observations.lock().unwrap();
        let value = values
            .get_mut(&(tenant.into(), id.into()))
            .ok_or(EnvironmentError::NotFound)?;
        value.phase = EnvironmentPhase::Paused;
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
        template_ref: adx_agent_core::TemplateRef::Version("1".into()),
        binding_id: id.into(),
    }
}
#[tokio::test]
async fn replicas_and_unknown_outcomes_keep_the_committed_identity() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let a = Activator::new(state.clone(), platform.clone());
    let b = Activator::new(state.clone(), platform.clone());
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&template()))
        .await
        .unwrap();
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
    assert_ne!(
        fresh.current.environment_id,
        original.current.environment_id
    );
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
    service
        .publish("tenant", &adx_agent_core::TemplateDraft::from(&template()))
        .await
        .unwrap();
    let sa = scope("a");
    let sb = scope("b");
    let (a, b) = tokio::join!(
        service.activate(&sa, None, adx_agent_core::unix_time_millis() + 60_000),
        service.activate(&sb, None, adx_agent_core::unix_time_millis() + 60_000)
    );
    let x = a.unwrap().binding;
    let y = b.unwrap().binding;
    assert_ne!(x.current.environment_id, y.current.environment_id);
    platform
        .observations
        .lock()
        .unwrap()
        .get_mut(&(x.scope.tenant.clone(), x.current.environment_id.clone()))
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
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&template()))
        .await
        .unwrap();
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
    activator
        .publish("tenant", &adx_agent_core::TemplateDraft::from(&template()))
        .await
        .unwrap();
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
    service
        .publish("tenant", &adx_agent_core::TemplateDraft::from(&template()))
        .await
        .unwrap();
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
        .remove(&(scope.tenant.clone(), old.current.environment_id.clone()));
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
    assert_ne!(fresh.current.environment_id, old.current.environment_id);
}

#[tokio::test]
async fn dormant_bindings_resolve_after_cache_loss_without_recreating_or_waking() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let service = Activator::new(state.clone(), platform.clone());
    service
        .publish("tenant", &adx_agent_core::TemplateDraft::from(&template()))
        .await
        .unwrap();
    let scope = scope("sleep");
    let original = service
        .activate(&scope, None, unix_time_millis() + 60_000)
        .await
        .unwrap();
    for phase in ["pausing", "paused", "resuming"] {
        let observation: EnvironmentObservation = serde_json::from_value(serde_json::json!({
            "id":original.binding.current.environment_id, "tenant":"tenant", "phase":phase,
            "ready":false, "runtime_id":null, "message":null
        }))
        .unwrap();
        platform.observations.lock().unwrap().insert(
            (
                "tenant".into(),
                original.binding.current.environment_id.clone(),
            ),
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
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&template()))
        .await
        .unwrap();
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
        observations[&("tenant".into(), old.current.environment_id)].phase,
        EnvironmentPhase::Deleted
    );
    assert_eq!(
        observations[&("tenant".into(), fresh.current.environment_id)].phase,
        EnvironmentPhase::Running
    );
}

#[tokio::test]
async fn alias_switch_keeps_logical_identity_and_restart_finishes_retirement() {
    use adx_agent_core::alias::{AliasConfig, VersionWeight};
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let a = Activator::new(state.clone(), platform.clone());
    let one = template();
    let mut two = one.clone();
    two.version = "2".into();
    two.image = "app:2".into();
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&one))
        .await
        .unwrap();
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&two))
        .await
        .unwrap();
    let mut config = AliasConfig {
        tenant: "tenant".into(),
        template: "app".into(),
        alias: "production".into(),
        seed: "stable".into(),
        versions: vec![VersionWeight {
            version: "1".into(),
            weight: 1,
        }],
    };
    let revision = state.put_alias(&config, None).await.unwrap();
    let mut scope = scope("workspace");
    scope.template_ref = TemplateRef::Alias("production".into());
    let deadline = adx_agent_core::unix_time_millis() + 10_000;
    let original = a.activate(&scope, None, deadline).await.unwrap().binding;
    platform
        .observations
        .lock()
        .unwrap()
        .get_mut(&("tenant".into(), original.current.environment_id.clone()))
        .unwrap()
        .phase = EnvironmentPhase::Paused;
    config.versions = vec![VersionWeight {
        version: "2".into(),
        weight: 1,
    }];
    state.put_alias(&config, Some(&revision)).await.unwrap();
    assert!(!a.reconcile_binding(&scope, deadline).await.unwrap());
    let switched = state.binding(&scope).await.unwrap().unwrap();
    assert_eq!(switched.generation, original.generation);
    assert_eq!(switched.current.version, "2");
    assert_eq!(
        switched.secondary.as_ref().unwrap().environment,
        original.current
    );
    let fresh = a.activate(&scope, None, deadline).await.unwrap();
    assert_eq!(
        fresh.binding.current, switched.current,
        "local hot cache must see its own switch"
    );
    platform.lose_delete_response.store(true, Ordering::SeqCst);
    assert!(a.reconcile_binding(&scope, deadline).await.is_err());
    assert!(state
        .binding(&scope)
        .await
        .unwrap()
        .unwrap()
        .secondary
        .is_some());
    let restarted = Activator::new(state.clone(), platform.clone());
    assert!(!restarted.reconcile_binding(&scope, deadline).await.unwrap());
    assert!(restarted.reconcile_binding(&scope, deadline).await.unwrap());
    assert!(state
        .binding(&scope)
        .await
        .unwrap()
        .unwrap()
        .secondary
        .is_none());
    assert_eq!(
        platform.observations.lock().unwrap()[&("tenant".into(), original.current.environment_id)]
            .phase,
        EnvironmentPhase::Deleted
    );
}

#[tokio::test]
async fn late_candidate_creation_after_target_change_is_cleaned_using_its_saved_id() {
    use adx_agent_core::alias::{AliasConfig, VersionWeight};
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let activator = Arc::new(Activator::new(state.clone(), platform.clone()));
    activator
        .publish("tenant", &adx_agent_core::TemplateDraft::from(&template()))
        .await
        .unwrap();
    let mut two = template();
    two.version = "2".into();
    two.image = "app:2".into();
    activator
        .publish("tenant", &adx_agent_core::TemplateDraft::from(&two))
        .await
        .unwrap();
    let mut config = AliasConfig {
        tenant: "tenant".into(),
        template: "app".into(),
        alias: "production".into(),
        seed: "stable".into(),
        versions: vec![VersionWeight {
            version: "1".into(),
            weight: 1,
        }],
    };
    let revision = state.put_alias(&config, None).await.unwrap();
    let mut scope = scope("workspace");
    scope.template_ref = TemplateRef::Alias("production".into());
    let deadline = unix_time_millis() + 5000;
    let original = activator
        .activate(&scope, None, deadline)
        .await
        .unwrap()
        .binding;
    platform
        .observations
        .lock()
        .unwrap()
        .get_mut(&("tenant".into(), original.current.environment_id.clone()))
        .unwrap()
        .phase = EnvironmentPhase::Paused;
    config.versions[0].version = "2".into();
    let revision = state.put_alias(&config, Some(&revision)).await.unwrap();
    let gate = Arc::new(tokio::sync::Notify::new());
    *platform.create_gate.lock().unwrap() = Some(gate.clone());
    let task = {
        let activator = activator.clone();
        let scope = scope.clone();
        tokio::spawn(async move { activator.reconcile_binding(&scope, deadline).await })
    };
    platform.create_entered.notified().await;
    let prepared = state.binding(&scope).await.unwrap().unwrap();
    let candidate = prepared.secondary.unwrap().environment.environment_id;
    config.versions[0].version = "1".into();
    state.put_alias(&config, Some(&revision)).await.unwrap();
    // Another owner observes the latest config, deletes the absent candidate and clears its reference.
    activator.reconcile_binding(&scope, deadline).await.unwrap();
    assert!(state
        .binding(&scope)
        .await
        .unwrap()
        .unwrap()
        .secondary
        .is_none());
    gate.notify_one();
    assert!(task.await.unwrap().is_err());
    let observations = platform.observations.lock().unwrap();
    assert_eq!(
        observations[&("tenant".into(), candidate)].phase,
        EnvironmentPhase::Deleted
    );
    assert_eq!(
        observations[&("tenant".into(), original.current.environment_id)].phase,
        EnvironmentPhase::Paused
    );
}

#[tokio::test]
async fn listing_missing_template_or_alias_preserves_not_found() {
    let activator = Activator::new(
        AgentState::new(Arc::new(MemoryRepository::default())),
        Arc::new(Platform::default()),
    );
    for template_ref in [
        TemplateRef::Version("999".into()),
        TemplateRef::Alias("missing".into()),
    ] {
        let query = adx_agent_core::activator::BindingList {
            tenant: "tenant".into(),
            template: "app".into(),
            template_ref,
            page_size: 50,
            page_token: None,
        };
        assert!(matches!(
            activator.list_bindings(&query).await,
            Err(Error::NotFound)
        ));
    }
}

/// A dormant previous Environment is checkpointed immediately when the candidate is
/// ready, so the switched Environment is Paused (storage only, zero runtime) rather
/// than idling until the template pause timeout fires.
#[tokio::test]
async fn dormant_previous_environment_checkpoints_candidate_before_switch() {
    use adx_agent_core::alias::{AliasConfig, VersionWeight};
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let a = Activator::new(state.clone(), platform.clone());
    let one = template();
    let mut two = one.clone();
    two.version = "2".into();
    two.image = "app:2".into();
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&one))
        .await
        .unwrap();
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&two))
        .await
        .unwrap();
    let mut config = AliasConfig {
        tenant: "tenant".into(),
        template: "app".into(),
        alias: "production".into(),
        seed: "stable".into(),
        versions: vec![VersionWeight {
            version: "1".into(),
            weight: 1,
        }],
    };
    let revision = state.put_alias(&config, None).await.unwrap();
    let mut scope = scope("dormant-upgrade");
    scope.template_ref = TemplateRef::Alias("production".into());
    let deadline = adx_agent_core::unix_time_millis() + 10_000;
    let original = a.activate(&scope, None, deadline).await.unwrap().binding;
    // The previous Environment is dormant: paused before the switch is attempted.
    platform
        .observations
        .lock()
        .unwrap()
        .get_mut(&("tenant".into(), original.current.environment_id.clone()))
        .unwrap()
        .phase = EnvironmentPhase::Paused;
    config.versions = vec![VersionWeight {
        version: "2".into(),
        weight: 1,
    }];
    state.put_alias(&config, Some(&revision)).await.unwrap();
    assert!(!a.reconcile_binding(&scope, deadline).await.unwrap());
    let switched = state.binding(&scope).await.unwrap().unwrap();
    assert_eq!(switched.generation, original.generation);
    assert_eq!(switched.current.version, "2");
    // The candidate was checkpointed to Paused, not left Running.
    let observations = platform.observations.lock().unwrap();
    assert_eq!(
        observations[&("tenant".into(), switched.current.environment_id.clone())].phase,
        EnvironmentPhase::Paused,
        "dormant upgrade must checkpoint the candidate instead of leaving it idle"
    );
    assert!(!observations[&("tenant".into(), switched.current.environment_id.clone())].ready);
}

/// An active previous Environment keeps the candidate Running so live traffic skips a
/// wake restore when the switch commits. This models the race where a user returns during
/// the candidate's boot: E1 was Paused when creation was admitted, but is Running again by
/// the time the candidate is ready.
#[tokio::test]
async fn active_previous_environment_keeps_candidate_running_before_switch() {
    use adx_agent_core::alias::{AliasConfig, VersionWeight};
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let a = Activator::new(state.clone(), platform.clone());
    let one = template();
    let mut two = one.clone();
    two.version = "2".into();
    two.image = "app:2".into();
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&one))
        .await
        .unwrap();
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&two))
        .await
        .unwrap();
    let mut config = AliasConfig {
        tenant: "tenant".into(),
        template: "app".into(),
        alias: "production".into(),
        seed: "stable".into(),
        versions: vec![VersionWeight {
            version: "1".into(),
            weight: 1,
        }],
    };
    let revision = state.put_alias(&config, None).await.unwrap();
    let mut scope = scope("active-upgrade");
    scope.template_ref = TemplateRef::Alias("production".into());
    let deadline = adx_agent_core::unix_time_millis() + 10_000;
    let original = a.activate(&scope, None, deadline).await.unwrap().binding;
    // Admit candidate creation by pausing E1, then prepare the candidate.
    platform
        .observations
        .lock()
        .unwrap()
        .get_mut(&("tenant".into(), original.current.environment_id.clone()))
        .unwrap()
        .phase = EnvironmentPhase::Paused;
    config.versions = vec![VersionWeight {
        version: "2".into(),
        weight: 1,
    }];
    state.put_alias(&config, Some(&revision)).await.unwrap();
    let prepared = state.prepare_binding(&scope).await.unwrap();
    let candidate = prepared.secondary.as_ref().unwrap().environment.clone();
    // The candidate already exists and is Running (get-first returns it without re-gating
    // on E1). A user returned during boot: E1 is awake again at the decision point.
    platform.observations.lock().unwrap().insert(
        ("tenant".into(), candidate.environment_id.clone()),
        EnvironmentObservation {
            id: candidate.environment_id.clone(),
            tenant: "tenant".into(),
            phase: EnvironmentPhase::Running,
            ready: true,
            runtime_id: Some("runtime".into()),
            message: None,
            resume_pending: false,
        },
    );
    platform
        .observations
        .lock()
        .unwrap()
        .get_mut(&("tenant".into(), original.current.environment_id.clone()))
        .unwrap()
        .phase = EnvironmentPhase::Running;
    assert!(!a.reconcile_binding(&scope, deadline).await.unwrap());
    let switched = state.binding(&scope).await.unwrap().unwrap();
    assert_eq!(switched.generation, original.generation);
    assert_eq!(switched.current.version, "2");
    assert_eq!(switched.current, candidate);
    let observations = platform.observations.lock().unwrap();
    assert_eq!(
        observations[&("tenant".into(), switched.current.environment_id.clone())].phase,
        EnvironmentPhase::Running,
        "active upgrade must keep the candidate Running to avoid a wake restore"
    );
    assert!(observations[&("tenant".into(), switched.current.environment_id.clone())].ready);
}

/// A Paused E1 whose wake is in flight (`resume_pending`) is NOT dormant: a user has already
/// arrived and is waiting (queued or blocked on an exclusive Volume). Checkpointing the
/// candidate would force that user through an extra wake restore after the switch, so the
/// active path must keep E2 Running. This is the resume-aware refinement of the dormant
/// check: phase is Paused, but a wake intent is registered, so the candidate is not paused.
#[tokio::test]
async fn paused_previous_environment_with_pending_resume_takes_the_active_path() {
    use adx_agent_core::alias::{AliasConfig, VersionWeight};
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let a = Activator::new(state.clone(), platform.clone());
    let one = template();
    let mut two = one.clone();
    two.version = "2".into();
    two.image = "app:2".into();
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&one))
        .await
        .unwrap();
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&two))
        .await
        .unwrap();
    let mut config = AliasConfig {
        tenant: "tenant".into(),
        template: "app".into(),
        alias: "production".into(),
        seed: "stable".into(),
        versions: vec![VersionWeight {
            version: "1".into(),
            weight: 1,
        }],
    };
    let revision = state.put_alias(&config, None).await.unwrap();
    let mut scope = scope("paused-e1-resume-pending");
    scope.template_ref = TemplateRef::Alias("production".into());
    let deadline = adx_agent_core::unix_time_millis() + 10_000;
    let original = a.activate(&scope, None, deadline).await.unwrap().binding;
    // E1 is Paused (so creation gate admits the candidate), but a wake is in flight: a user
    // has arrived and Platform has a registered resume intent (queued or blocked on a Volume).
    {
        let mut observations = platform.observations.lock().unwrap();
        let e1 = observations
            .get_mut(&("tenant".into(), original.current.environment_id.clone()))
            .unwrap();
        e1.phase = EnvironmentPhase::Paused;
        e1.resume_pending = true;
    }
    config.versions = vec![VersionWeight {
        version: "2".into(),
        weight: 1,
    }];
    state.put_alias(&config, Some(&revision)).await.unwrap();
    let prepared = state.prepare_binding(&scope).await.unwrap();
    let candidate = prepared.secondary.as_ref().unwrap().environment.clone();
    platform.observations.lock().unwrap().insert(
        ("tenant".into(), candidate.environment_id.clone()),
        EnvironmentObservation {
            id: candidate.environment_id.clone(),
            tenant: "tenant".into(),
            phase: EnvironmentPhase::Running,
            ready: true,
            runtime_id: Some("runtime".into()),
            message: None,
            resume_pending: false,
        },
    );
    assert!(!a.reconcile_binding(&scope, deadline).await.unwrap());
    let switched = state.binding(&scope).await.unwrap().unwrap();
    assert_eq!(switched.current.version, "2");
    assert_eq!(switched.current, candidate);
    let observations = platform.observations.lock().unwrap();
    assert_eq!(
        observations[&("tenant".into(), switched.current.environment_id.clone())].phase,
        EnvironmentPhase::Running,
        "a Paused E1 with a pending wake takes the active path: candidate stays Running"
    );
    assert!(
        observations[&("tenant".into(), switched.current.environment_id.clone())].ready,
        "the candidate must NOT be checkpointed when a wake for E1 is in flight"
    );
    // pause_count stays zero: checkpoint_candidate was skipped because E1 is not dormant.
    assert_eq!(
        platform.pause_count.load(Ordering::SeqCst),
        0,
        "no pause must be issued while E1 has a pending wake"
    );
}

/// The creation gate treats a Paused E1 with `resume_pending` as Stopped-dormant, NOT Idle:
/// a Paused E1 has already released its runtime, so a wake in flight must not block candidate
/// creation. This is the resume-aware refinement that keeps `resume_pending` out of the
/// creation gate (creation.rs) and confined to the switch-time checkpoint decision (lib.rs).
///
/// Unlike `paused_previous_environment_with_pending_resume_takes_the_active_path`, this test
/// does NOT pre-save the candidate via `prepare_binding`, so `reconcile_binding` drives the
/// full creation path and the creation gate fires. A regression that put `resume_pending` back
/// into the creation gate would defer creation here (PreviousEnvironmentActive) and the switch
/// would not commit in this pass.
#[tokio::test]
async fn paused_previous_environment_with_pending_resume_does_not_block_creation() {
    use adx_agent_core::alias::{AliasConfig, VersionWeight};
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let a = Activator::new(state.clone(), platform.clone());
    let one = template();
    let mut two = one.clone();
    two.version = "2".into();
    two.image = "app:2".into();
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&one))
        .await
        .unwrap();
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&two))
        .await
        .unwrap();
    let mut config = AliasConfig {
        tenant: "tenant".into(),
        template: "app".into(),
        alias: "production".into(),
        seed: "stable".into(),
        versions: vec![VersionWeight {
            version: "1".into(),
            weight: 1,
        }],
    };
    let revision = state.put_alias(&config, None).await.unwrap();
    let mut scope = scope("creation-gate-pending-resume");
    scope.template_ref = TemplateRef::Alias("production".into());
    let deadline = adx_agent_core::unix_time_millis() + 10_000;
    let original = a.activate(&scope, None, deadline).await.unwrap().binding;
    // E1 is Paused (so the creation gate's Stopped check admits it) but a wake is in flight.
    // The creation gate must NOT treat `resume_pending` as active: a Paused E1 has released its
    // runtime, so candidate creation proceeds regardless of an in-flight wake for E1.
    {
        let mut observations = platform.observations.lock().unwrap();
        let e1 = observations
            .get_mut(&("tenant".into(), original.current.environment_id.clone()))
            .unwrap();
        e1.phase = EnvironmentPhase::Paused;
        e1.resume_pending = true;
    }
    config.versions = vec![VersionWeight {
        version: "2".into(),
        weight: 1,
    }];
    state.put_alias(&config, Some(&revision)).await.unwrap();
    // Full path: reconcile creates the candidate through the creation gate, then the switch-time
    // decision point runs in the same pass. The switch commits because the gate admitted E2.
    assert!(!a.reconcile_binding(&scope, deadline).await.unwrap());
    let switched = state.binding(&scope).await.unwrap().unwrap();
    assert_eq!(switched.generation, original.generation);
    assert_eq!(switched.current.version, "2");
    // The decision point then sees E1 as Paused + resume_pending = NOT Idle-dormant, so it keeps
    // the candidate Running (the active path) instead of checkpointing it.
    let observations = platform.observations.lock().unwrap();
    assert_eq!(
        observations[&("tenant".into(), switched.current.environment_id.clone())].phase,
        EnvironmentPhase::Running,
        "creation must not be deferred by E1's pending wake"
    );
    assert!(
        observations[&("tenant".into(), switched.current.environment_id.clone())].ready,
        "the candidate must be created and ready in this pass"
    );
    assert_eq!(
        platform.pause_count.load(Ordering::SeqCst),
        0,
        "no checkpoint: E1 has a pending wake, so the decision point takes the active path"
    );
}

/// A Failed previous Environment is intentionally NOT treated as dormant: Activator does
/// not do health recovery or second-guess Platform failure semantics, so a Failed E1 takes
/// the active path — the candidate stays Running and the switch commits without pausing it.
/// This covers the `previous_environment_dormant` branch that Paused/Deleted matching skips.
#[tokio::test]
async fn failed_previous_environment_takes_the_active_path_without_checkpointing() {
    use adx_agent_core::alias::{AliasConfig, VersionWeight};
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let a = Activator::new(state.clone(), platform.clone());
    let one = template();
    let mut two = one.clone();
    two.version = "2".into();
    two.image = "app:2".into();
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&one))
        .await
        .unwrap();
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&two))
        .await
        .unwrap();
    let mut config = AliasConfig {
        tenant: "tenant".into(),
        template: "app".into(),
        alias: "production".into(),
        seed: "stable".into(),
        versions: vec![VersionWeight {
            version: "1".into(),
            weight: 1,
        }],
    };
    let revision = state.put_alias(&config, None).await.unwrap();
    let mut scope = scope("failed-e1-upgrade");
    scope.template_ref = TemplateRef::Alias("production".into());
    let deadline = adx_agent_core::unix_time_millis() + 10_000;
    let original = a.activate(&scope, None, deadline).await.unwrap().binding;
    // Admit candidate creation by pausing E1 first, then prepare the candidate.
    platform
        .observations
        .lock()
        .unwrap()
        .get_mut(&("tenant".into(), original.current.environment_id.clone()))
        .unwrap()
        .phase = EnvironmentPhase::Paused;
    config.versions = vec![VersionWeight {
        version: "2".into(),
        weight: 1,
    }];
    state.put_alias(&config, Some(&revision)).await.unwrap();
    let prepared = state.prepare_binding(&scope).await.unwrap();
    let candidate = prepared.secondary.as_ref().unwrap().environment.clone();
    // Candidate exists and is Running. At the decision point E1 is Failed (not Paused).
    platform.observations.lock().unwrap().insert(
        ("tenant".into(), candidate.environment_id.clone()),
        EnvironmentObservation {
            id: candidate.environment_id.clone(),
            tenant: "tenant".into(),
            phase: EnvironmentPhase::Running,
            ready: true,
            runtime_id: Some("runtime".into()),
            message: None,
            resume_pending: false,
        },
    );
    platform
        .observations
        .lock()
        .unwrap()
        .get_mut(&("tenant".into(), original.current.environment_id.clone()))
        .unwrap()
        .phase = EnvironmentPhase::Failed;
    assert!(!a.reconcile_binding(&scope, deadline).await.unwrap());
    let switched = state.binding(&scope).await.unwrap().unwrap();
    assert_eq!(switched.current.version, "2");
    assert_eq!(switched.current, candidate);
    let observations = platform.observations.lock().unwrap();
    assert_eq!(
        observations[&("tenant".into(), switched.current.environment_id.clone())].phase,
        EnvironmentPhase::Running,
        "a Failed E1 takes the active path: candidate stays Running, no checkpoint"
    );
    assert!(observations[&("tenant".into(), switched.current.environment_id.clone())].ready);
}

/// A checkpoint capture slower than the reconciliation step budget must not strand the
/// upgrade: `checkpoint_candidate` reports the missed step, and the next pass observes the
/// candidate already Paused (the Platform committed it) and commits the switch. This is the
/// convergence path review 4(b) requires: the client budget is clamped to `timeout_seconds`
/// (the step's remaining budget), so the outer reconciliation timeout bounds the capture
/// instead of overshooting it.
#[tokio::test]
async fn slow_checkpoint_converges_on_the_next_pass_after_the_step_budget_drops() {
    use adx_agent_core::alias::{AliasConfig, VersionWeight};
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let a = Activator::new(state.clone(), platform.clone());
    let one = template();
    let mut two = one.clone();
    two.version = "2".into();
    two.image = "app:2".into();
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&one))
        .await
        .unwrap();
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&two))
        .await
        .unwrap();
    let mut config = AliasConfig {
        tenant: "tenant".into(),
        template: "app".into(),
        alias: "production".into(),
        seed: "stable".into(),
        versions: vec![VersionWeight {
            version: "1".into(),
            weight: 1,
        }],
    };
    let revision = state.put_alias(&config, None).await.unwrap();
    let mut scope = scope("slow-checkpoint");
    scope.template_ref = TemplateRef::Alias("production".into());
    let deadline = adx_agent_core::unix_time_millis() + 10_000;
    let original = a.activate(&scope, None, deadline).await.unwrap().binding;
    // E1 is dormant: the candidate is checkpointed on the switch attempt.
    platform
        .observations
        .lock()
        .unwrap()
        .get_mut(&("tenant".into(), original.current.environment_id.clone()))
        .unwrap()
        .phase = EnvironmentPhase::Paused;
    config.versions = vec![VersionWeight {
        version: "2".into(),
        weight: 1,
    }];
    state.put_alias(&config, Some(&revision)).await.unwrap();
    // The first checkpoint never completes within the step: pause blocks forever, so the
    // step budget (the reconciliation timeout wrapping reconcile_binding, mirroring
    // reconciliation.rs) drops the call. The Platform has, however, durably published
    // Paused by the second pass.
    platform.pause_blocks.store(true, Ordering::SeqCst);
    let first = tokio::time::timeout(
        Duration::from_millis(150),
        a.reconcile_binding(&scope, deadline),
    )
    .await;
    platform.pause_blocks.store(false, Ordering::SeqCst);
    // The step reported the missed capture; it did not commit the switch.
    assert!(
        first.is_err(),
        "a blocked checkpoint must surface as a step failure, not a silent switch: {first:?}"
    );
    let binding_after = state.binding(&scope).await.unwrap().unwrap();
    assert_eq!(
        binding_after.current.version, "1",
        "the switch must not commit while the capture is in flight"
    );
    let candidate = binding_after.secondary.unwrap().environment.clone();
    // Platform committed Paused out-of-band while the step was stuck; the next pass
    // observes the Paused candidate and commits the switch without re-attempting pause.
    platform
        .observations
        .lock()
        .unwrap()
        .get_mut(&("tenant".into(), candidate.environment_id.clone()))
        .unwrap()
        .phase = EnvironmentPhase::Paused;
    assert!(!a.reconcile_binding(&scope, deadline).await.unwrap());
    let switched = state.binding(&scope).await.unwrap().unwrap();
    assert_eq!(switched.current.version, "2");
    let observations = platform.observations.lock().unwrap();
    assert_eq!(
        observations[&("tenant".into(), switched.current.environment_id.clone())].phase,
        EnvironmentPhase::Paused,
        "the slow checkpoint still converges to a Paused switched Environment"
    );
}

/// P3-b: when the reconciliation deadline has already expired by the time
/// `checkpoint_candidate` runs, the step must NOT fire a pointless pause request.
/// `pause_timeout_seconds` clamps an expired budget back to 1s, so an explicit
/// `unix_time_millis() >= deadline` guard must precede it. The candidate is ready and E1 is
/// dormant (the dormant branch is taken); the only thing that should abort the pause is the
/// expired-deadline guard. Verified by asserting `pause_count` stays zero and the result is
/// `OutcomeUnknown`.
#[tokio::test]
async fn checkpoint_is_skipped_when_the_step_deadline_already_expired() {
    use adx_agent_core::alias::{AliasConfig, VersionWeight};
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let a = Activator::new(state.clone(), platform.clone());
    let one = template();
    let mut two = one.clone();
    two.version = "2".into();
    two.image = "app:2".into();
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&one))
        .await
        .unwrap();
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&two))
        .await
        .unwrap();
    let mut config = AliasConfig {
        tenant: "tenant".into(),
        template: "app".into(),
        alias: "production".into(),
        seed: "stable".into(),
        versions: vec![VersionWeight {
            version: "1".into(),
            weight: 1,
        }],
    };
    let revision = state.put_alias(&config, None).await.unwrap();
    let mut scope = scope("expired-checkpoint-deadline");
    scope.template_ref = TemplateRef::Alias("production".into());
    let deadline = adx_agent_core::unix_time_millis() + 10_000;
    let original = a.activate(&scope, None, deadline).await.unwrap().binding;
    // E1 is dormant: the candidate is checkpointed on the switch attempt.
    platform
        .observations
        .lock()
        .unwrap()
        .get_mut(&("tenant".into(), original.current.environment_id.clone()))
        .unwrap()
        .phase = EnvironmentPhase::Paused;
    config.versions = vec![VersionWeight {
        version: "2".into(),
        weight: 1,
    }];
    state.put_alias(&config, Some(&revision)).await.unwrap();
    let prepared = state.prepare_binding(&scope).await.unwrap();
    let candidate = prepared.secondary.as_ref().unwrap().environment.clone();
    // Candidate already observed as ready/Running: `activate_environment` returns its
    // cached observation at the creation gate (creation.rs) without reaching the creation-
    // time dormant recheck, so the slow GET below is exercised only at the switch-time
    // `previous_environment_dormant` call (lib.rs).
    platform.observations.lock().unwrap().insert(
        ("tenant".into(), candidate.environment_id.clone()),
        EnvironmentObservation {
            id: candidate.environment_id.clone(),
            tenant: "tenant".into(),
            phase: EnvironmentPhase::Running,
            ready: true,
            runtime_id: Some("runtime".into()),
            message: None,
            resume_pending: false,
        },
    );
    // The switch-time dormant recheck GETs E1; make that call sleep past the step deadline so
    // `checkpoint_candidate` observes an already-expired deadline and must skip the pause.
    platform.slow_get.lock().unwrap().replace((
        original.current.environment_id.clone(),
        Duration::from_millis(250),
    ));
    // Comfortably after activate_environment (microseconds, in-memory) but well before the
    // 250 ms dormant-recheck sleep completes.
    let near_deadline = adx_agent_core::unix_time_millis() + 100;
    let result = a.reconcile_binding(&scope, near_deadline).await;
    assert!(
        matches!(&result, Err(Error::OutcomeUnknown(_))),
        "an expired checkpoint deadline must surface as OutcomeUnknown, not commit the switch: {result:?}"
    );
    assert_eq!(
        platform.pause_count.load(Ordering::SeqCst),
        0,
        "checkpoint_candidate must not fire pause after the step deadline expired"
    );
    // The switch did not commit, and the candidate remains the saved secondary.
    let binding_after = state.binding(&scope).await.unwrap().unwrap();
    assert_eq!(
        binding_after.current.version, "1",
        "the switch must not commit when the checkpoint deadline expired"
    );
    let _ = candidate; // candidate identity is the saved secondary; nothing to assert here
}
