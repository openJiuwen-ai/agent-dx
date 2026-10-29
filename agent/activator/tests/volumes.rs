use adx_activator::Activator;
use adx_agent_core::{
    environment::*,
    error::Error,
    volumes::{VolumeRef, VolumeSpec},
    *,
};
use adx_agent_store::{AgentState, MemoryRepository};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Platform {
    owners: Mutex<BTreeSet<String>>,
    executions: Mutex<BTreeMap<String, EnvironmentObservation>>,
    fail_create: std::sync::atomic::AtomicBool,
    ensure_error: Mutex<Option<EnvironmentError>>,
    ensure_calls: std::sync::atomic::AtomicUsize,
}
#[async_trait::async_trait]
impl Environment for Platform {
    async fn ensure_volume(
        &self,
        _: &str,
        owner: &str,
        spec: &VolumeSpec,
    ) -> Result<VolumeRef, EnvironmentError> {
        self.ensure_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Some(error) = self.ensure_error.lock().unwrap().clone() {
            return Err(error);
        }
        assert_eq!(spec.id, "workspace-user");
        self.owners.lock().unwrap().insert(owner.into());
        Ok(VolumeRef {
            id: spec.id.clone(),
            uid: "fixed-uid".into(),
        })
    }
    async fn release_volumes(&self, _: &str, owner: &str) -> Result<(), EnvironmentError> {
        self.owners.lock().unwrap().remove(owner);
        Ok(())
    }
    async fn create(
        &self,
        request: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        assert!(self.owners.lock().unwrap().contains(&request.id));
        assert_eq!(
            request.execution.mounts[0].volume_uid.as_deref(),
            Some("fixed-uid")
        );
        if self.fail_create.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(EnvironmentError::Unavailable(
                "coordinator restarting".into(),
            ));
        }
        let record = EnvironmentObservation {
            id: request.id.clone(),
            tenant: request.tenant.clone(),
            phase: EnvironmentPhase::Running,
            ready: true,
            runtime_id: None,
            message: None,
        };
        self.executions
            .lock()
            .unwrap()
            .insert(request.id.clone(), record.clone());
        Ok(record)
    }
    async fn get(
        &self,
        _: &str,
        id: &str,
    ) -> Result<Option<EnvironmentObservation>, EnvironmentError> {
        Ok(self.executions.lock().unwrap().get(id).cloned())
    }
    async fn delete(&self, _: &str, id: &str) -> Result<EnvironmentObservation, EnvironmentError> {
        let mut record = self
            .executions
            .lock()
            .unwrap()
            .remove(id)
            .ok_or(EnvironmentError::NotFound)?;
        record.phase = EnvironmentPhase::Deleted;
        record.ready = false;
        Ok(record)
    }
}
fn template() -> TemplateVersion {
    serde_json::from_value(serde_json::json!({"name":"app","version":"1","image":"image","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":128},
        "volumes":[{"id":"workspace-${user}","source":{"type":"directory","storage_domain":"sfs","path":"workspaces/tenant/${user}"}}],
        "mounts":[{"type":"bind","volume_id":"workspace-${user}","target":"/workspace","options":["rw"]}]
    })).unwrap()
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
async fn activation_ensures_before_create_and_deletion_releases_only_its_binding() {
    let platform = Arc::new(Platform::default());
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let a = Activator::new(state.clone(), platform.clone());
    let b = Activator::new(state, platform.clone());
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&template()))
        .await
        .unwrap();
    let mut ids = Vec::new();
    for id in ["a", "b"] {
        let binding = a
            .activate_with_parameters(
                &scope(id),
                None,
                unix_time_millis() + 60_000,
                false,
                Some(b"user=user"),
            )
            .await
            .unwrap()
            .binding;
        assert_eq!(
            binding.volumes.get("workspace-user").map(String::as_str),
            Some("fixed-uid"),
            "activation response must include the committed Volume UID"
        );
        ids.push(binding.current.environment_id);
    }
    assert_eq!(platform.owners.lock().unwrap().len(), 2);
    b.delete_binding(&scope("a")).await.unwrap();
    assert_eq!(*platform.owners.lock().unwrap(), [ids[1].clone()].into());
    b.delete_binding(&scope("b")).await.unwrap();
    assert!(platform.owners.lock().unwrap().is_empty());
}
#[tokio::test(start_paused = true)]
async fn activation_deadline_keeps_binding_volume_association_until_explicit_deletion() {
    let platform = Arc::new(Platform::default());
    platform
        .fail_create
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let a = Activator::new(
        AgentState::new(Arc::new(MemoryRepository::default())),
        platform.clone(),
    );
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&template()))
        .await
        .unwrap();
    let started = tokio::time::Instant::now();
    assert!(a
        .activate_with_parameters(
            &scope("a"),
            None,
            unix_time_millis() + 80,
            false,
            Some(b"user=user")
        )
        .await
        .is_err());
    assert!(started.elapsed() <= std::time::Duration::from_millis(80));
    let binding = a.binding(&scope("a")).await.unwrap();
    assert!(platform
        .owners
        .lock()
        .unwrap()
        .contains(&binding.current.environment_id));
    a.delete_binding(&scope("a")).await.unwrap();
    assert!(platform.owners.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn deterministic_volume_refusals_do_not_consume_the_activation_retry_budget() {
    for error in [
        EnvironmentError::Invalid("credential rejected".into()),
        EnvironmentError::Conflict("identity mismatch".into()),
        EnvironmentError::NotFound,
    ] {
        let platform = Arc::new(Platform::default());
        *platform.ensure_error.lock().unwrap() = Some(error);
        let activator = Activator::new(
            AgentState::new(Arc::new(MemoryRepository::default())),
            platform.clone(),
        );
        activator
            .publish("tenant", &adx_agent_core::TemplateDraft::from(&template()))
            .await
            .unwrap();
        let started = tokio::time::Instant::now();
        let result = activator
            .activate_with_parameters(
                &scope("a"),
                None,
                unix_time_millis() + 60_000,
                false,
                Some(b"user=user"),
            )
            .await;
        assert!(
            matches!(
                result,
                Err(Error::Invalid(_) | Error::Conflict(_) | Error::NotFound)
            ),
            "{result:?}"
        );
        assert_eq!(
            platform
                .ensure_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(started.elapsed(), std::time::Duration::ZERO);
        assert!(platform.executions.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn alias_upgrade_preserves_volume_uid_and_releases_only_the_retired_owner() {
    use adx_agent_core::alias::{AliasConfig, VersionWeight};
    let platform = Arc::new(Platform::default());
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let activator = Activator::new(state.clone(), platform.clone());
    activator
        .publish("tenant", &adx_agent_core::TemplateDraft::from(&template()))
        .await
        .unwrap();
    let mut two = template();
    two.version = "2".into();
    two.image = "image:2".into();
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
    activator
        .activate_with_parameters(&scope, None, deadline, false, Some(b"user=user"))
        .await
        .unwrap();
    let original = state.binding(&scope).await.unwrap().unwrap();
    assert_eq!(
        original.volumes.get("workspace-user").map(String::as_str),
        Some("fixed-uid")
    );
    platform
        .executions
        .lock()
        .unwrap()
        .get_mut(&original.current.environment_id)
        .unwrap()
        .phase = EnvironmentPhase::Paused;
    config.versions = vec![VersionWeight {
        version: "2".into(),
        weight: 1,
    }];
    state.put_alias(&config, Some(&revision)).await.unwrap();
    activator.reconcile_binding(&scope, deadline).await.unwrap();
    let switched = state.binding(&scope).await.unwrap().unwrap();
    assert_eq!(switched.current.version, "2");
    assert_eq!(switched.volumes, original.volumes);
    assert_eq!(
        platform.owners.lock().unwrap().len(),
        2,
        "new owner must exist before old release"
    );
    activator.reconcile_binding(&scope, deadline).await.unwrap();
    assert_eq!(
        *platform.owners.lock().unwrap(),
        [switched.current.environment_id].into()
    );
    activator.delete_binding(&scope).await.unwrap();
    assert!(platform.owners.lock().unwrap().is_empty());
}
