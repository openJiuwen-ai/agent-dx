use super::*;
use crate::ingress::accounts::{
    ClaimAssignment, ClaimProgress, LoginRequest, LoginResponse, QuotaResponse,
};
use adx_agent_api::local::LocalControl;
use adx_agent_core::{environment::*, mounts::Parameters, TemplateDraft};
use adx_agent_store::{AgentState, MemoryRepository};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

#[derive(Default)]
struct Auth {
    state: Mutex<ClaimState>,
    slow: AtomicUsize,
    records: AtomicUsize,
}
impl Auth {
    async fn delay(&self, stage: usize) {
        if self.slow.load(Ordering::SeqCst) == stage {
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }
}
#[async_trait::async_trait]
impl BusinessAuth for Auth {
    async fn login(&self, _: LoginRequest) -> accounts::Result<LoginResponse> {
        Err(AccountError::Unavailable)
    }
    async fn authenticate(&self, _: &str) -> accounts::Result<Principal> {
        Ok(principal())
    }
    async fn check(&self, _: &Principal) -> accounts::Result<()> {
        Ok(())
    }
    async fn logout(&self, _: &str) -> accounts::Result<()> {
        Ok(())
    }
    async fn quota(&self, _: &Principal) -> accounts::Result<QuotaResponse> {
        Err(AccountError::Unavailable)
    }
    async fn model_parameters(&self, _: &Principal) -> accounts::Result<Parameters> {
        Ok(Parameters::new())
    }
    async fn claim_state(&self, _: &Principal, _: &Scope) -> accounts::Result<ClaimState> {
        self.delay(1).await;
        Ok(self.state.lock().unwrap().clone())
    }
    async fn record_claim(
        &self,
        _: &Principal,
        scope: &Scope,
        claim: &str,
        slot: &str,
        workspace: &str,
        _: &str,
    ) -> accounts::Result<ClaimProgress> {
        self.delay(2).await;
        self.records.fetch_add(1, Ordering::SeqCst);
        *self.state.lock().unwrap() = ClaimState {
            credential_version: Some(claim.into()),
            credential_ready: false,
            assignment: Some(ClaimAssignment {
                tenant: scope.tenant.clone(),
                template: scope.template.clone(),
                workspace: workspace.into(),
                slot_id: slot.into(),
                claim_id: claim.into(),
                committed: false,
            }),
        };
        Ok(ClaimProgress {
            credential_ready: false,
            assignment_committed: false,
        })
    }
    async fn activate_claim(
        &self,
        _: &Principal,
        _: &Scope,
        _: &str,
        _: &str,
        _: &str,
    ) -> accounts::Result<()> {
        self.delay(3).await;
        self.state.lock().unwrap().credential_ready = true;
        Ok(())
    }
    async fn commit_claim(&self, _: &Principal, _: &Scope, _: &str) -> accounts::Result<()> {
        self.delay(4).await;
        self.state
            .lock()
            .unwrap()
            .assignment
            .as_mut()
            .unwrap()
            .committed = true;
        Ok(())
    }
}

struct Platform;
#[async_trait::async_trait]
impl Environment for Platform {
    async fn get(
        &self,
        _: &str,
        _: &str,
    ) -> Result<Option<EnvironmentObservation>, EnvironmentError> {
        Ok(None)
    }
    async fn create(
        &self,
        _: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        Err(EnvironmentError::NotFound)
    }
    async fn delete(&self, _: &str, _: &str) -> Result<EnvironmentObservation, EnvironmentError> {
        Err(EnvironmentError::NotFound)
    }
}

fn principal() -> Principal {
    Principal {
        user_id: "alice".into(),
        expires_at: i64::MAX,
        token_digest: "test".into(),
    }
}
fn scope() -> Scope {
    Scope {
        tenant: "tenant".into(),
        template: "app".into(),
        template_ref: TemplateRef::Alias("production".into()),
        binding_id: "digitalmate-alice".into(),
    }
}
fn config() -> PoolConfig {
    serde_json::from_value(serde_json::json!({
        "tenant":"tenant","pool_id":"pool","template":"app","version":"1","enabled":true,
        "min_running":1,"min_ready":1,"max_slots":3,"max_provisioning":1,
        "key_confirm_timeout_seconds":60,"provision_timeout_seconds":300,"claim_timeout_seconds":30,"max_ready_age_seconds":86400
    })).unwrap()
}
async fn fixture(ready: bool) -> (AgentState, PoolRuntime, ManagedService, Arc<Auth>) {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let draft: TemplateDraft = serde_json::from_value(serde_json::json!({"name":"app","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":500,"memory_mib":512}})).unwrap();
    state.publish("tenant", &draft).await.unwrap();
    state.publish("tenant", &draft).await.unwrap();
    state.put_alias(&serde_json::from_value(serde_json::json!({"tenant":"tenant","template":"app","alias":"production","seed":"stable","versions":[{"version":"1","weight":1}]})).unwrap(), None).await.unwrap();
    state.put_pool(&config(), None).await.unwrap();
    if ready {
        let slot = state
            .reserve_pool_slot(
                "tenant",
                "pool",
                "https://model/v1",
                adx_agent_core::unix_time_millis(),
            )
            .await
            .unwrap()
            .unwrap();
        state
            .confirm_pool_key(
                "tenant",
                "pool",
                &slot.slot_id,
                &slot.key_digest,
                adx_agent_core::unix_time_millis(),
            )
            .await
            .unwrap();
        state
            .mark_pool_slot_ready("tenant", "pool", &slot.slot_id, SlotPhase::Running)
            .await
            .unwrap();
    }
    let control: Arc<dyn Control> = Arc::new(LocalControl::new(Arc::new(
        adx_activator::Activator::new(state.clone(), Arc::new(Platform)),
    )));
    let auth = Arc::new(Auth::default());
    let runtime = PoolRuntime {
        control: control.clone(),
        auth: auth.clone(),
        tenant: "tenant".into(),
        pools_by_version: BTreeMap::from([("1".into(), "pool".into())]),
        claim_budget: Duration::from_millis(100),
        _supplier: SupplierTask { task: None },
    };
    (state, runtime, ManagedService::new(control), auth)
}

#[tokio::test]
async fn empty_disabled_and_mismatched_pools_allow_cold_start() {
    for case in 0..3 {
        let (state, runtime, managed, auth) = fixture(false).await;
        let (revision, mut config) = state.pool("tenant", "pool").await.unwrap().unwrap();
        if case == 1 {
            config.enabled = false;
        }
        if case == 2 {
            config.version = "2".into();
        }
        state.put_pool(&config, Some(&revision)).await.unwrap();
        assert!(runtime
            .prepare(
                &Arc::new(RequestContext::new(Duration::from_secs(1))),
                &scope(),
                &principal(),
                &managed
            )
            .await
            .is_ok());
        assert_eq!(auth.records.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn alias_change_resumes_original_claim_and_waits_for_release_without_new_mapping() {
    let (state, mut runtime, managed, auth) = fixture(true).await;
    let claim = state
        .claim_pool_slot(
            "tenant",
            "pool",
            &scope(),
            &scope().binding_id,
            adx_agent_core::unix_time_millis(),
        )
        .await
        .unwrap()
        .unwrap();
    let ClaimOutcome::Granted {
        slot_id,
        parameters,
        ..
    } = claim
    else {
        panic!("grant required")
    };
    auth.record_claim(
        &principal(),
        &scope(),
        &scope().binding_id,
        &slot_id,
        &parameters["workspace"],
        &parameters["model_api_key"],
    )
    .await
    .unwrap();
    let (revision, mut alias) = state
        .alias("tenant", "app", "production")
        .await
        .unwrap()
        .unwrap();
    alias.versions[0].version = "2".into();
    state.put_alias(&alias, Some(&revision)).await.unwrap();
    runtime.pools_by_version.clear();
    let ctx = Arc::new(RequestContext::new(Duration::from_secs(1)));
    assert!(runtime
        .prepare(&ctx, &scope(), &principal(), &managed)
        .await
        .is_err());
    state
        .abort_pool_claim("tenant", "pool", &scope().binding_id, Some("timeout"))
        .await
        .unwrap();
    assert!(runtime
        .prepare(&ctx, &scope(), &principal(), &managed)
        .await
        .is_err());
    state
        .retire_pool_slot("tenant", "pool", &slot_id)
        .await
        .unwrap();
    assert!(runtime
        .prepare(&ctx, &scope(), &principal(), &managed)
        .await
        .is_ok());
    assert!(state.retired_pool_keys("tenant", "pool", 20).await.unwrap()[0].preserve_key);
}

#[tokio::test]
async fn whole_claim_flow_obeys_parent_and_claim_deadlines() {
    for stage in 1..=4 {
        let (_, mut runtime, managed, auth) = fixture(true).await;
        runtime.claim_budget = Duration::from_millis(30);
        auth.slow.store(stage, Ordering::SeqCst);
        let started = tokio::time::Instant::now();
        assert!(runtime
            .prepare(
                &Arc::new(RequestContext::new(Duration::from_secs(5))),
                &scope(),
                &principal(),
                &managed
            )
            .await
            .is_err());
        assert!(started.elapsed() < Duration::from_secs(1));
    }
    let (_, runtime, managed, auth) = fixture(true).await;
    auth.slow.store(1, Ordering::SeqCst);
    let started = tokio::time::Instant::now();
    assert!(runtime
        .prepare(
            &Arc::new(RequestContext::new(Duration::from_millis(15))),
            &scope(),
            &principal(),
            &managed
        )
        .await
        .is_err());
    assert!(started.elapsed() < Duration::from_millis(90));
}

#[tokio::test]
async fn partial_account_claim_is_retryable_and_never_cold_starts() {
    let (_, runtime, managed, auth) = fixture(true).await;
    auth.state.lock().unwrap().credential_version = Some(scope().binding_id);
    assert!(runtime
        .prepare(
            &Arc::new(RequestContext::new(Duration::from_secs(1))),
            &scope(),
            &principal(),
            &managed
        )
        .await
        .is_err());
    assert_eq!(auth.records.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn concurrent_requests_share_one_claim_and_binding() {
    let (state, runtime, managed, _) = fixture(true).await;
    let ctx = Arc::new(RequestContext::new(Duration::from_secs(1)));
    let scope = scope();
    let principal = principal();
    let (left, right) = tokio::join!(
        runtime.prepare(&ctx, &scope, &principal, &managed),
        runtime.prepare(&ctx, &scope, &principal, &managed)
    );
    assert!(left.is_ok() && right.is_ok());
    assert!(state.binding(&scope).await.unwrap().is_some());
    assert_eq!(
        state
            .pool_status("tenant", "pool")
            .await
            .unwrap()
            .counts
            .claims_committed,
        1
    );
}

struct CleanupModels(AtomicUsize);
#[async_trait::async_trait]
impl accounts::ModelProvider for CleanupModels {
    async fn ensure(&self, _: &str, _: &str, _: &str) -> accounts::Result<()> {
        unreachable!()
    }
    async fn quota(&self, _: &str, _: &str) -> accounts::Result<QuotaResponse> {
        unreachable!()
    }
    async fn release_pool(&self, _: &str, _: &str) -> accounts::Result<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn supplier_never_deletes_a_key_after_a_grant_even_before_transfer() {
    let (state, runtime, _, auth) = fixture(true).await;
    let claim = state
        .claim_pool_slot(
            "tenant",
            "pool",
            &scope(),
            &scope().binding_id,
            adx_agent_core::unix_time_millis(),
        )
        .await
        .unwrap()
        .unwrap();
    let ClaimOutcome::Granted {
        slot_id,
        parameters,
        ..
    } = claim
    else {
        panic!("grant required")
    };
    auth.record_claim(
        &principal(),
        &scope(),
        &scope().binding_id,
        &slot_id,
        &parameters["workspace"],
        &parameters["model_api_key"],
    )
    .await
    .unwrap();
    state
        .abort_pool_claim("tenant", "pool", &scope().binding_id, Some("timeout"))
        .await
        .unwrap();
    state
        .retire_pool_slot("tenant", "pool", &slot_id)
        .await
        .unwrap();
    let (revision, mut config) = state.pool("tenant", "pool").await.unwrap().unwrap();
    config.enabled = false;
    state.put_pool(&config, Some(&revision)).await.unwrap();
    let models = Arc::new(CleanupModels(AtomicUsize::new(0)));
    supply(
        &runtime.control,
        &(models.clone() as Arc<dyn accounts::ModelProvider>),
        &Arc::new(RequestContext::new(Duration::from_secs(1))),
        "tenant",
        "pool",
        "https://model/v1",
    )
    .await
    .unwrap();
    assert_eq!(models.0.load(Ordering::SeqCst), 0);
    assert!(state
        .retired_pool_keys("tenant", "pool", 20)
        .await
        .unwrap()
        .is_empty());
}
