use adx_activator::Activator;
use adx_agent_core::{
    environment::*,
    pool::*,
    volumes::{VolumeRef, VolumeSpec},
    Scope, TemplateRef,
};
use adx_agent_store::{AgentState, MemoryRepository};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Platform {
    environments: Mutex<BTreeMap<String, EnvironmentObservation>>,
    volumes: Mutex<Vec<String>>,
    creates: Mutex<Vec<CreateEnvironment>>,
    releases: Mutex<Vec<String>>,
}
#[async_trait::async_trait]
impl Environment for Platform {
    async fn ensure_volume(
        &self,
        _tenant: &str,
        _owner: &str,
        spec: &VolumeSpec,
    ) -> Result<VolumeRef, EnvironmentError> {
        self.volumes.lock().unwrap().push(spec.id.clone());
        Ok(VolumeRef {
            id: spec.id.clone(),
            uid: format!("uid-{}", spec.id),
        })
    }
    async fn create(
        &self,
        request: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        self.creates.lock().unwrap().push(request.clone());
        let observation = EnvironmentObservation {
            id: request.id.clone(),
            tenant: request.tenant.clone(),
            phase: EnvironmentPhase::Running,
            ready: true,
            runtime_id: None,
            message: None,
        };
        self.environments
            .lock()
            .unwrap()
            .insert(request.id.clone(), observation.clone());
        Ok(observation)
    }
    async fn get(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<EnvironmentObservation>, EnvironmentError> {
        let observations = self.environments.lock().unwrap();
        Ok(observations
            .get(id)
            .filter(|observation| observation.tenant == tenant)
            .cloned())
    }
    async fn delete(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        let mut observations = self.environments.lock().unwrap();
        let Some(observation) = observations.get_mut(id) else {
            return Err(EnvironmentError::NotFound);
        };
        if observation.tenant != tenant {
            return Err(EnvironmentError::NotFound);
        }
        observation.phase = EnvironmentPhase::Deleted;
        observation.ready = false;
        Ok(observation.clone())
    }
    async fn release_volumes(&self, _tenant: &str, owner: &str) -> Result<(), EnvironmentError> {
        self.releases.lock().unwrap().push(owner.into());
        Ok(())
    }
}

fn template() -> adx_agent_core::TemplateVersion {
    let draft: adx_agent_core::TemplateDraft = serde_json::from_value(serde_json::json!({
        "name": "digitalmate",
        "image": "digitalmate:v19",
        "isolation_runtime": "runsc",
        "resources": {"cpu_millis": 500, "memory_mib": 1024},
        "env": {
            "API_BASE": "${model_api_base}",
            "API_KEY": "${model_api_key}",
            "DIGITALMATE_WORKSPACE": "/data/state"
        },
        "service": [{"protocol": "ws", "port": 18092}],
        "mounts": [{
            "type": "bind",
            "target": "/data/state",
            "volume_id": "digitalmate-workspace-${workspace}",
            "options": ["rw"]
        }],
        "volumes": [{
            "id": "digitalmate-workspace-${workspace}",
            "access_mode": "exclusive",
            "create_if_missing": true,
            "source": {
                "type": "directory",
                "storage_domain": "sfs",
                "path": "workspaces/${workspace}"
            }
        }]
    }))
    .unwrap();
    draft.with_version("1".into())
}
fn config() -> PoolConfig {
    PoolConfig {
        pool_id: "digitalmate-v17".into(),
        tenant: "tenant".into(),
        template: "digitalmate".into(),
        version: "1".into(),
        enabled: true,
        min_running: 1,
        min_ready: 1,
        max_slots: 4,
        max_provisioning: 1,
        key_confirm_timeout_seconds: 60,
        provision_timeout_seconds: 300,
        claim_timeout_seconds: 30,
        max_ready_age_seconds: 86_400,
    }
}
fn scope() -> Scope {
    Scope {
        tenant: "tenant".into(),
        template: "digitalmate".into(),
        template_ref: TemplateRef::Version("1".into()),
        binding_id: "digitalmate-alice".into(),
    }
}

#[tokio::test]
async fn prewarmed_slot_is_provisioned_before_it_is_claimed() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let activator = Activator::new(state.clone(), platform.clone());
    activator
        .publish("tenant", &adx_agent_core::TemplateDraft::from(&template()))
        .await
        .unwrap();
    activator
        .put_pool(&PoolPutRequest {
            config: config(),
            expected_revision: None,
        })
        .await
        .unwrap();
    let reserved = activator
        .reserve_pool_slot(&ReserveRequest {
            tenant: "tenant".into(),
            pool_id: "digitalmate-v17".into(),
            model_api_base: "https://model.example/v1".into(),
        })
        .await
        .unwrap()
        .expect("reserved");
    assert_eq!(reserved.state, SlotState::KeyPending);
    let pending = activator
        .pending_pool_keys(&PendingKeysRequest {
            tenant: "tenant".into(),
            pool_id: "digitalmate-v17".into(),
            limit: 20,
        })
        .await
        .unwrap();
    assert_eq!(pending.slots.len(), 1);
    activator
        .confirm_pool_key(&KeyConfirmedRequest {
            tenant: "tenant".into(),
            pool_id: "digitalmate-v17".into(),
            slot_id: reserved.slot_id.clone(),
            key_digest: reserved.key_digest.clone(),
        })
        .await
        .unwrap();
    activator
        .reconcile_pool(
            "tenant",
            "digitalmate-v17",
            adx_agent_core::unix_time_millis() + 60_000,
        )
        .await
        .unwrap();
    let status = activator
        .pool_status("tenant", "digitalmate-v17")
        .await
        .unwrap();
    assert_eq!(status.counts.ready_running, 1);
    assert_eq!(platform.creates.lock().unwrap().len(), 1);
    let created = platform.creates.lock().unwrap()[0].clone();
    assert_eq!(created.id, reserved.environment_id);
    assert_eq!(
        created.execution.env["API_BASE"],
        "https://model.example/v1"
    );
    assert!(created.execution.env["API_KEY"].starts_with("sk-"));
    assert_eq!(
        created.execution.env["DIGITALMATE_WORKSPACE"],
        "/data/state"
    );
    let mount = &created.execution.mounts[0];
    let volume_id = mount.volume_id.as_deref().unwrap();
    assert!(volume_id.starts_with("digitalmate-workspace-pool-s-"));
    assert_eq!(
        mount.volume_uid.as_deref(),
        Some(format!("uid-{volume_id}").as_str())
    );
    // The slot is only deliverable after the pool created the complete execution.
    let claim = activator
        .claim_pool(&ClaimRequest {
            scope: scope(),
            pool_id: "digitalmate-v17".into(),
            claim_id: scope().binding_id,
        })
        .await
        .unwrap();
    let ClaimOutcome::Granted { parameters, .. } = claim else {
        panic!("expected grant");
    };
    assert_eq!(parameters["workspace"], reserved.workspace);
    let committed = activator
        .commit_pool(&CommitRequest {
            tenant: "tenant".into(),
            pool_id: "digitalmate-v17".into(),
            claim_id: scope().binding_id,
        })
        .await
        .unwrap();
    let ClaimOutcome::Committed { binding, .. } = committed else {
        panic!("expected commit");
    };
    assert_eq!(binding.current.environment_id, reserved.environment_id);
    assert_eq!(binding.parameters["workspace"], reserved.workspace);
}

#[tokio::test]
async fn quarantined_slot_is_deleted_and_only_its_volume_owner_is_released() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let activator = Activator::new(state.clone(), platform.clone());
    activator
        .publish("tenant", &adx_agent_core::TemplateDraft::from(&template()))
        .await
        .unwrap();
    activator
        .put_pool(&PoolPutRequest {
            config: config(),
            expected_revision: None,
        })
        .await
        .unwrap();
    let reserved = activator
        .reserve_pool_slot(&ReserveRequest {
            tenant: "tenant".into(),
            pool_id: "digitalmate-v17".into(),
            model_api_base: "https://model.example/v1".into(),
        })
        .await
        .unwrap()
        .unwrap();
    activator
        .confirm_pool_key(&KeyConfirmedRequest {
            tenant: "tenant".into(),
            pool_id: "digitalmate-v17".into(),
            slot_id: reserved.slot_id.clone(),
            key_digest: reserved.key_digest.clone(),
        })
        .await
        .unwrap();
    activator
        .reconcile_pool(
            "tenant",
            "digitalmate-v17",
            adx_agent_core::unix_time_millis() + 60_000,
        )
        .await
        .unwrap();
    let claim = activator
        .claim_pool(&ClaimRequest {
            scope: scope(),
            pool_id: "digitalmate-v17".into(),
            claim_id: scope().binding_id,
        })
        .await
        .unwrap();
    assert!(matches!(claim, ClaimOutcome::Granted { .. }));
    activator
        .abort_pool(&AbortRequest {
            tenant: "tenant".into(),
            pool_id: "digitalmate-v17".into(),
            claim_id: scope().binding_id,
            reason: Some("account_conflict".into()),
        })
        .await
        .unwrap();
    activator
        .reconcile_pool(
            "tenant",
            "digitalmate-v17",
            adx_agent_core::unix_time_millis() + 60_000,
        )
        .await
        .unwrap();
    assert_eq!(
        platform.releases.lock().unwrap().as_slice(),
        std::slice::from_ref(&reserved.environment_id)
    );
    let retired = activator
        .retired_pool_keys(&RetiredRequest {
            tenant: "tenant".into(),
            pool_id: "digitalmate-v17".into(),
            limit: 20,
        })
        .await
        .unwrap();
    assert_eq!(retired.slots.len(), 1);
    let claim = state
        .pool_claim("tenant", "digitalmate-v17", &scope().binding_id)
        .await
        .unwrap()
        .unwrap()
        .1;
    assert!(claim.released);
    assert!(retired.slots[0].preserve_key);
}

#[tokio::test]
async fn shrink_retires_surplus_running_slots_and_protects_claiming() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let activator = Activator::new(state.clone(), platform.clone());
    activator
        .publish("tenant", &adx_agent_core::TemplateDraft::from(&template()))
        .await
        .unwrap();
    let mut initial = config();
    initial.min_ready = 8;
    initial.max_slots = 8;
    initial.max_provisioning = 8;
    state.put_pool(&initial, None).await.unwrap();
    for _ in 0..8 {
        let slot = state
            .reserve_pool_slot(
                "tenant",
                "digitalmate-v17",
                "https://model/v1",
                adx_agent_core::unix_time_millis(),
            )
            .await
            .unwrap()
            .unwrap();
        state
            .confirm_pool_key(
                "tenant",
                "digitalmate-v17",
                &slot.slot_id,
                &slot.key_digest,
                adx_agent_core::unix_time_millis(),
            )
            .await
            .unwrap();
    }
    activator
        .reconcile_pool(
            "tenant",
            "digitalmate-v17",
            adx_agent_core::unix_time_millis() + 60_000,
        )
        .await
        .unwrap();
    let claim = state
        .claim_pool_slot(
            "tenant",
            "digitalmate-v17",
            &scope(),
            &scope().binding_id,
            adx_agent_core::unix_time_millis(),
        )
        .await
        .unwrap()
        .unwrap();
    let ClaimOutcome::Granted { environment_id, .. } = claim else {
        panic!("expected grant")
    };
    let (revision, _) = state
        .pool("tenant", "digitalmate-v17")
        .await
        .unwrap()
        .unwrap();
    let mut smaller = config();
    smaller.max_slots = 2;
    state.put_pool(&smaller, Some(&revision)).await.unwrap();
    activator
        .reconcile_pool(
            "tenant",
            "digitalmate-v17",
            adx_agent_core::unix_time_millis() + 60_000,
        )
        .await
        .unwrap();
    let status = state
        .pool_status("tenant", "digitalmate-v17")
        .await
        .unwrap();
    assert_eq!(status.counts.ready_running, 1);
    assert_eq!(status.counts.claiming, 1);
    assert_eq!(platform.releases.lock().unwrap().len(), 6);
    assert!(!platform.releases.lock().unwrap().contains(&environment_id));
}

#[tokio::test]
async fn unconfirmed_create_absence_never_releases_a_workspace() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let activator = Activator::new(state.clone(), platform.clone());
    activator
        .publish("tenant", &adx_agent_core::TemplateDraft::from(&template()))
        .await
        .unwrap();
    state.put_pool(&config(), None).await.unwrap();
    let slot = state
        .reserve_pool_slot(
            "tenant",
            "digitalmate-v17",
            "https://model/v1",
            adx_agent_core::unix_time_millis(),
        )
        .await
        .unwrap()
        .unwrap();
    state
        .confirm_pool_key(
            "tenant",
            "digitalmate-v17",
            &slot.slot_id,
            &slot.key_digest,
            adx_agent_core::unix_time_millis(),
        )
        .await
        .unwrap();
    state
        .begin_pool_provisioning("tenant", "digitalmate-v17", &slot.slot_id)
        .await
        .unwrap();
    state
        .quarantine_pool_slot("tenant", "digitalmate-v17", &slot.slot_id, "unknown_create")
        .await
        .unwrap();
    assert!(matches!(
        activator
            .reconcile_pool(
                "tenant",
                "digitalmate-v17",
                adx_agent_core::unix_time_millis() + 60_000
            )
            .await,
        Err(adx_activator::Error::OutcomeUnknown(_))
    ));
    assert!(platform.releases.lock().unwrap().is_empty());
    assert!(state
        .pool_slot("tenant", "digitalmate-v17", &slot.slot_id)
        .await
        .unwrap()
        .is_some());
}
