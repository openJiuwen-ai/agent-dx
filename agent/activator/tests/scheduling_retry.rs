//! Component contracts with durable product metadata and a fake Environment capability.
use adx_activator::{Activator, Error};
use adx_agent_core::{
    alias::{AliasConfig, VersionWeight},
    environment::*,
    volumes::{VolumeRef, VolumeSpec},
    *,
};
use adx_agent_store::{
    AgentState, Check, Key, MemoryRepository, Put, Record, Repository, Transaction,
};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Platform {
    records: Mutex<BTreeMap<String, EnvironmentObservation>>,
    creates: Mutex<Vec<CreateEnvironment>>,
    errors: Mutex<VecDeque<EnvironmentError>>,
}
fn observed(id: &str, phase: EnvironmentPhase) -> EnvironmentObservation {
    EnvironmentObservation {
        id: id.into(),
        tenant: "tenant".into(),
        ready: phase == EnvironmentPhase::Running,
        phase,
        runtime_id: None,
        message: None,
    }
}
#[async_trait::async_trait]
impl Environment for Platform {
    async fn ensure_volume(
        &self,
        _: &str,
        _: &str,
        spec: &VolumeSpec,
    ) -> Result<VolumeRef, EnvironmentError> {
        Ok(VolumeRef {
            id: spec.id.clone(),
            uid: "uid".into(),
        })
    }
    async fn create(
        &self,
        r: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        self.creates.lock().unwrap().push(r.clone());
        if let Some(error) = self.errors.lock().unwrap().pop_front() {
            if matches!(error, EnvironmentError::OutcomeUnknown(_)) {
                self.records
                    .lock()
                    .unwrap()
                    .insert(r.id.clone(), observed(&r.id, EnvironmentPhase::Creating));
            }
            return Err(error);
        }
        let record = observed(&r.id, EnvironmentPhase::Running);
        self.records
            .lock()
            .unwrap()
            .insert(r.id.clone(), record.clone());
        Ok(record)
    }
    async fn get(
        &self,
        _: &str,
        id: &str,
    ) -> Result<Option<EnvironmentObservation>, EnvironmentError> {
        Ok(self.records.lock().unwrap().get(id).cloned())
    }
    async fn delete(&self, _: &str, id: &str) -> Result<EnvironmentObservation, EnvironmentError> {
        self.records.lock().unwrap().remove(id);
        Ok(observed(id, EnvironmentPhase::Deleted))
    }
}
struct Fixture {
    repo: Arc<MemoryRepository>,
    state: AgentState,
    platform: Arc<Platform>,
    scope: Scope,
    binding: AgentBinding,
    candidate: EnvironmentRef,
}
impl Fixture {
    async fn new(volume_mode: Option<&str>) -> Self {
        let repo = Arc::new(MemoryRepository::default());
        let state = AgentState::new(repo.clone());
        for _ in 0..2 {
            let mut draft = serde_json::json!({"name":"agent","image":"image","isolation_runtime":"runsc",
                "resources":{"cpu_millis":100,"memory_mib":128}});
            if let Some(mode) = volume_mode {
                draft["volumes"] = serde_json::json!([{"id":"workspace", "access_mode":mode,
                    "source":{"type":"directory","storage_domain":"sfs","path":"tenant/workspace"}}]);
                draft["mounts"] =
                    serde_json::json!([{"type":"bind","volume_id":"workspace","target":"/data"}]);
            }
            state
                .publish("tenant", &serde_json::from_value(draft).unwrap())
                .await
                .unwrap();
        }
        let mut alias = AliasConfig {
            tenant: "tenant".into(),
            template: "agent".into(),
            alias: "live".into(),
            seed: "seed".into(),
            versions: vec![VersionWeight {
                version: "1".into(),
                weight: 1,
            }],
        };
        let rev = state.put_alias(&alias, None).await.unwrap();
        let scope = Scope {
            tenant: "tenant".into(),
            template: "agent".into(),
            template_ref: TemplateRef::Alias("live".into()),
            binding_id: "user".into(),
        };
        let old = state.create_binding(scope.clone()).await.unwrap();
        alias.versions[0].version = "2".into();
        state.put_alias(&alias, Some(&rev)).await.unwrap();
        let binding = state.prepare_binding(&scope).await.unwrap();
        let candidate = binding.secondary.as_ref().unwrap().environment.clone();
        let platform = Arc::new(Platform::default());
        platform.records.lock().unwrap().insert(
            old.current.environment_id.clone(),
            observed(&old.current.environment_id, EnvironmentPhase::Running),
        );
        Self {
            repo,
            state,
            platform,
            scope,
            binding,
            candidate,
        }
    }
    fn activator(&self) -> Activator {
        Activator::new(self.state.clone(), self.platform.clone())
    }
    async fn reconcile(&self) -> Result<bool, Error> {
        self.activator()
            .reconcile_binding(&self.scope, unix_time_millis() + 1000)
            .await
    }
    async fn due(&self) {
        let key = Key::new("binding", &["tenant", "agent", "alias", "live", "user"]).unwrap();
        let record = self.repo.get(&key).await.unwrap().unwrap();
        let mut binding: AgentBinding = record.decode().unwrap();
        binding
            .creation_attempts
            .get_mut(&self.candidate.environment_id)
            .unwrap()
            .retry_after_unix_ms = 0;
        assert!(self
            .repo
            .commit(
                &Transaction::new(
                    vec![Check {
                        key: key.clone(),
                        expected: Some(record.revision)
                    }],
                    vec![Put {
                        key,
                        record: Record::new(&binding).unwrap()
                    }]
                )
                .unwrap()
            )
            .await
            .unwrap());
    }
}
#[tokio::test]
async fn cancelled_attempt_changes_request_identity_and_survives_activator_restart() {
    let f = Fixture::new(None).await;
    f.platform.records.lock().unwrap().insert(
        f.binding.current.environment_id.clone(),
        observed(&f.binding.current.environment_id, EnvironmentPhase::Paused),
    );
    f.platform
        .errors
        .lock()
        .unwrap()
        .push_back(EnvironmentError::SchedulingCancelled(Box::default()));
    assert!(matches!(
        f.reconcile().await,
        Err(Error::CreationDeferred { .. })
    ));
    let saved = f.state.binding(&f.scope).await.unwrap().unwrap();
    let attempt = &saved.creation_attempts[&f.candidate.environment_id];
    assert_eq!((attempt.number, attempt.backoff_seconds), (1, 5));
    assert_eq!(saved.current, f.binding.current);
    assert_eq!(saved.secondary, f.binding.secondary);
    assert!(matches!(
        f.reconcile().await,
        Err(Error::CreationDeferred { .. })
    ));
    assert_eq!(
        f.platform.creates.lock().unwrap().len(),
        1,
        "backoff must survive another Activator instance"
    );
    f.platform.records.lock().unwrap().insert(
        f.binding.current.environment_id.clone(),
        observed(&f.binding.current.environment_id, EnvironmentPhase::Paused),
    );
    f.due().await;
    assert!(!f.reconcile().await.unwrap());
    let calls = f.platform.creates.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].id, calls[1].id);
    assert_eq!(calls[0].execution, calls[1].execution);
    assert_eq!(
        (calls[0].creation_attempt, calls[1].creation_attempt),
        (0, 1)
    );
    assert_ne!(
        adx_activator::environment_request::creation_request_id(&calls[0]).unwrap(),
        adx_activator::environment_request::creation_request_id(&calls[1]).unwrap()
    );
}
#[tokio::test]
async fn cancelled_candidate_waits_for_old_env_pause_without_another_create() {
    let f = Fixture::new(Some("exclusive")).await;
    f.platform.records.lock().unwrap().insert(
        f.binding.current.environment_id.clone(),
        observed(&f.binding.current.environment_id, EnvironmentPhase::Paused),
    );
    f.platform
        .errors
        .lock()
        .unwrap()
        .push_back(EnvironmentError::SchedulingCancelled(Box::default()));
    assert!(matches!(
        f.reconcile().await,
        Err(Error::CreationDeferred { .. })
    ));
    assert_eq!(f.platform.creates.lock().unwrap().len(), 1);
    assert!(matches!(
        f.reconcile().await,
        Err(Error::CreationDeferred { .. })
    ));
    f.platform.records.lock().unwrap().insert(
        f.binding.current.environment_id.clone(),
        observed(&f.binding.current.environment_id, EnvironmentPhase::Running),
    );
    f.due().await;
    assert!(matches!(
        f.reconcile().await,
        Err(Error::CreationDeferred { .. })
    ));
    assert_eq!(f.platform.creates.lock().unwrap().len(), 1);
    let saved = f.state.binding(&f.scope).await.unwrap().unwrap();
    assert_eq!(
        saved.creation_attempts[&f.candidate.environment_id].number,
        1
    );
    assert_eq!(
        saved.creation_attempts[&f.candidate.environment_id].backoff_seconds,
        10
    );
    f.platform.records.lock().unwrap().insert(
        f.binding.current.environment_id.clone(),
        observed(&f.binding.current.environment_id, EnvironmentPhase::Paused),
    );
    f.due().await;
    assert!(!f.reconcile().await.unwrap());
    assert_eq!(f.platform.creates.lock().unwrap().len(), 2);
    assert_eq!(f.platform.creates.lock().unwrap()[1].creation_attempt, 1);
}
#[tokio::test]
async fn existing_starting_recovering_or_failed_execution_is_never_recreated() {
    for phase in [
        EnvironmentPhase::Creating,
        EnvironmentPhase::Resuming,
        EnvironmentPhase::Pausing,
        EnvironmentPhase::Failed,
    ] {
        let f = Fixture::new(Some("exclusive")).await;
        f.platform.records.lock().unwrap().insert(
            f.candidate.environment_id.clone(),
            observed(&f.candidate.environment_id, phase),
        );
        let _ = f.reconcile().await;
        assert!(f.platform.creates.lock().unwrap().is_empty(), "{phase:?}");
        assert!(f
            .state
            .binding(&f.scope)
            .await
            .unwrap()
            .unwrap()
            .creation_attempts
            .is_empty());
    }
}
#[tokio::test]
async fn lost_create_response_is_observed_without_advancing_request_attempt() {
    let f = Fixture::new(None).await;
    f.platform.records.lock().unwrap().insert(
        f.binding.current.environment_id.clone(),
        observed(&f.binding.current.environment_id, EnvironmentPhase::Paused),
    );
    f.platform
        .errors
        .lock()
        .unwrap()
        .push_back(EnvironmentError::OutcomeUnknown("lost reply".into()));
    assert!(!f.reconcile().await.unwrap());
    assert_eq!(f.platform.creates.lock().unwrap().len(), 1);
    assert!(f
        .state
        .binding(&f.scope)
        .await
        .unwrap()
        .unwrap()
        .creation_attempts
        .is_empty());
    assert_eq!(
        f.state.binding(&f.scope).await.unwrap().unwrap().current,
        f.binding.current
    );
}
#[tokio::test]
async fn changed_target_retires_waiter_instead_of_retrying_creation() {
    let f = Fixture::new(None).await;
    f.platform.records.lock().unwrap().insert(
        f.binding.current.environment_id.clone(),
        observed(&f.binding.current.environment_id, EnvironmentPhase::Paused),
    );
    f.platform
        .errors
        .lock()
        .unwrap()
        .push_back(EnvironmentError::SchedulingCancelled(Box::default()));
    assert!(matches!(
        f.reconcile().await,
        Err(Error::CreationDeferred { .. })
    ));
    let (rev, mut alias) = f
        .state
        .alias("tenant", "agent", "live")
        .await
        .unwrap()
        .unwrap();
    alias.versions[0].version = "1".into();
    f.state.put_alias(&alias, Some(&rev)).await.unwrap();
    assert!(!f.reconcile().await.unwrap());
    let saved = f.state.binding(&f.scope).await.unwrap().unwrap();
    assert_eq!(saved.current, f.binding.current);
    assert!(saved.secondary.is_none());
    assert!(saved.creation_attempts.is_empty());
    assert_eq!(f.platform.creates.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn cancelled_candidate_does_not_retry_while_old_execution_is_transitioning() {
    for phase in [
        EnvironmentPhase::Creating,
        EnvironmentPhase::Pausing,
        EnvironmentPhase::Resuming,
        EnvironmentPhase::Failed,
    ] {
        let f = Fixture::new(None).await;
        f.platform.records.lock().unwrap().insert(
            f.binding.current.environment_id.clone(),
            observed(&f.binding.current.environment_id, EnvironmentPhase::Paused),
        );
        f.platform
            .errors
            .lock()
            .unwrap()
            .push_back(EnvironmentError::SchedulingCancelled(Box::default()));
        assert!(matches!(
            f.reconcile().await,
            Err(Error::CreationDeferred { .. })
        ));
        f.platform.records.lock().unwrap().insert(
            f.binding.current.environment_id.clone(),
            observed(&f.binding.current.environment_id, phase),
        );
        f.due().await;
        assert!(
            matches!(f.reconcile().await, Err(Error::CreationDeferred { .. })),
            "{phase:?}"
        );
        assert_eq!(f.platform.creates.lock().unwrap().len(), 1, "{phase:?}");
        assert_eq!(
            f.state
                .binding(&f.scope)
                .await
                .unwrap()
                .unwrap()
                .creation_attempts[&f.candidate.environment_id]
                .number,
            1
        );
    }
}

#[tokio::test]
async fn candidate_creation_waits_for_previous_pause_for_every_volume_mode() {
    for mode in [None, Some("shared"), Some("exclusive")] {
        let f = Fixture::new(mode).await;
        assert!(
            matches!(f.reconcile().await, Err(Error::CreationDeferred { .. })),
            "{mode:?}"
        );
        assert!(f.platform.creates.lock().unwrap().is_empty(), "{mode:?}");
        let saved = f.state.binding(&f.scope).await.unwrap().unwrap();
        assert_eq!(
            saved.creation_attempts[&f.candidate.environment_id].number,
            0
        );
        f.due().await;
        assert!(
            matches!(f.reconcile().await, Err(Error::CreationDeferred { .. })),
            "{mode:?}"
        );
        assert!(f.platform.creates.lock().unwrap().is_empty(), "{mode:?}");
        f.platform.records.lock().unwrap().insert(
            f.binding.current.environment_id.clone(),
            observed(&f.binding.current.environment_id, EnvironmentPhase::Paused),
        );
        // A pause does not bypass the persisted check time.
        assert!(matches!(
            f.reconcile().await,
            Err(Error::CreationDeferred { .. })
        ));
        f.due().await;
        assert!(!f.reconcile().await.unwrap(), "{mode:?}");
        let calls = f.platform.creates.lock().unwrap();
        assert_eq!(calls.len(), 1, "{mode:?}");
        assert_eq!(
            calls[0].creation_attempt, 0,
            "waiting is not a cancelled creation"
        );
        assert_eq!(calls[0].id, f.candidate.environment_id);
    }
}
