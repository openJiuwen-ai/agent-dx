use adx_activator::Activator;
use adx_agent_core::{environment::*, *};
use adx_agent_store::{AgentState, MemoryRepository};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Capture(Mutex<Vec<CreateEnvironment>>, std::sync::atomic::AtomicBool);
#[async_trait::async_trait]
impl Environment for Capture {
    async fn golden_available(
        &self,
        _tenant: &str,
        _id: &str,
        _image: &str,
        _runtime: &str,
    ) -> Result<bool, EnvironmentError> {
        Ok(self.1.load(std::sync::atomic::Ordering::Relaxed))
    }
    async fn create(
        &self,
        request: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        self.0.lock().unwrap().push(request.clone());
        Ok(EnvironmentObservation {
            id: request.id.clone(),
            tenant: request.tenant.clone(),
            phase: EnvironmentPhase::Running,
            ready: true,
            runtime_id: None,
            message: None,
        })
    }
    async fn get(
        &self,
        _: &str,
        _: &str,
    ) -> Result<Option<EnvironmentObservation>, EnvironmentError> {
        Ok(None)
    }
    async fn delete(&self, _: &str, _: &str) -> Result<EnvironmentObservation, EnvironmentError> {
        Err(EnvironmentError::NotFound)
    }
}

#[tokio::test]
async fn first_decision_is_atomic_and_remains_tied_to_the_product_subject() {
    use adx_agent_core::activator::{ActivationRequest, ResolveRequest, ServiceSelector};
    use adx_agent_core::prewarm::{EligibleStart, PolicyScope, PolicyUpdate, PrewarmRecipe};
    use std::sync::atomic::Ordering;

    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Capture::default());
    platform.1.store(true, Ordering::Relaxed);
    let a = Activator::new(state.clone(), platform.clone());
    let b = Activator::new(state, platform.clone());
    let mut draft = TemplateDraft::from(&template());
    let digest = format!("sha256:{}", "a".repeat(64));
    draft.image = format!("app@{digest}");
    draft.service = vec![Service {
        protocol: Protocol::Ws,
        port: 8080,
    }];
    draft.prewarm_recipe = Some(PrewarmRecipe {
        snapshot_id: "snapshot-fixed".into(),
        image_digest: digest.clone(),
        restore_profile_digest: digest,
        handoff_contract_version: "1".into(),
        restore_env: [("APP_PHASE".into(), "restore".into())].into(),
    });
    a.publish("tenant", &draft).await.unwrap();
    let mut policy = PolicyUpdate {
        scope: PolicyScope {
            tenant: "tenant".into(),
            product: "app".into(),
        },
        expected_revision: None,
        enabled: true,
        template: "app".into(),
        version: "1".into(),
        validation_ref: "acceptance".into(),
    };
    let active = a.update_prewarm_policy(&policy).await.unwrap();
    let cut = active.policy.first_enabled_at_ms.unwrap();
    let mut request = ResolveRequest {
        activation: ActivationRequest {
            scope: scope(),
            expected_generation: None,
            bypass_cache: false,
            parameters: Some(b"workspace=w&model_url=https%3A%2F%2Fmodel.example".to_vec()),
            product_start: Some(EligibleStart {
                product: "app".into(),
                registered_at_ms: Some(cut + 1),
            }),
        },
        service: ServiceSelector {
            protocol: Protocol::Ws,
            port: Some(8080),
        },
    };
    let deadline = unix_time_millis() + 60000;
    let (left, right) = tokio::join!(a.resolve(&request, deadline), b.resolve(&request, deadline));
    let first = left.unwrap().target.binding;
    assert_eq!(first, right.unwrap().target.binding);
    assert_eq!(first.current.snapshot_id.as_deref(), Some("snapshot-fixed"));
    assert!(platform.0.lock().unwrap().iter().all(|r| r
        .execution
        .env
        .get("APP_PHASE")
        .is_some_and(|phase| phase == "restore")));

    // A new alias target or disable does not change an already fixed decision.
    let mut next = draft.clone();
    next.name = "next-template".into();
    a.publish("tenant", &next).await.unwrap();
    policy.expected_revision = Some(active.revision);
    policy.enabled = false;
    let disabled = a.update_prewarm_policy(&policy).await.unwrap();
    request.activation.scope.template = next.name;
    assert_eq!(
        b.resolve(&request, deadline).await.unwrap().target.binding,
        first
    );
    a.delete_binding(&first.scope).await.unwrap();
    policy.expected_revision = Some(disabled.revision);
    policy.enabled = true;
    policy.template = request.activation.scope.template.clone();
    let reenabled = a.update_prewarm_policy(&policy).await.unwrap();
    assert_eq!(reenabled.policy.first_enabled_at_ms, Some(cut));
    let new_lifecycle = a.resolve(&request, deadline).await.unwrap().target.binding;
    assert_ne!(new_lifecycle.generation, first.generation);
    assert!(new_lifecycle.current.snapshot_id.is_none());

    // Unavailable preflight downgrades a new user before any create, including misses.
    request.activation.scope.binding_id = "bob".into();
    platform.1.store(false, Ordering::Relaxed);
    let cold = a.resolve(&request, deadline).await.unwrap().target.binding;
    assert!(cold.current.snapshot_id.is_none());
    platform.1.store(true, Ordering::Relaxed);
    assert_eq!(
        b.resolve(&request, deadline).await.unwrap().target.binding,
        cold
    );
}
fn template() -> TemplateVersion {
    serde_json::from_value(
        serde_json::json!({"name":"app","version":"1","image":"app:1",
        "isolation_runtime":"runsc","resources":{"cpu_millis":250,"memory_mib":256},
        "env":{"API_BASE":"${model_url}","MODEL_NAME":"fixed"},
        "mounts":[{"type":"bind","host_path":"/mnt/users/${workspace}","target":"/home/agent"}]}),
    )
    .unwrap()
}
fn scope() -> Scope {
    Scope {
        tenant: "tenant".into(),
        template: "app".into(),
        template_ref: adx_agent_core::TemplateRef::Version("1".into()),
        binding_id: "alice".into(),
    }
}

#[tokio::test]
async fn initialize_once_and_ignore_even_malformed_parameters_across_activators() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Capture::default());
    let a = Activator::new(state.clone(), platform.clone());
    let b = Activator::new(state.clone(), platform.clone());
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&template()))
        .await
        .unwrap();
    let scope = scope();
    let first = a
        .activate_with_parameters(
            &scope,
            None,
            unix_time_millis() + 60000,
            false,
            Some(b"workspace=w-alice&model_url=https%3A%2F%2Fmodel.example%2Fv1"),
        )
        .await
        .unwrap();
    for parameters in [
        None,
        Some(b"%broken".as_slice()),
        Some(b"workspace=../../bad".as_slice()),
        Some(b"\xff".as_slice()),
    ] {
        let target = b
            .activate_with_parameters(&scope, None, unix_time_millis() + 60000, true, parameters)
            .await
            .unwrap();
        assert_eq!(target.binding, first.binding);
    }
    for request in platform.0.lock().unwrap().iter() {
        assert_eq!(request.execution.mounts[0].host_path, "/mnt/users/w-alice");
        assert_eq!(
            request.execution.env["API_BASE"],
            "https://model.example/v1"
        );
        assert_eq!(request.execution.env["MODEL_NAME"], "fixed");
    }
    b.delete_binding(&scope).await.unwrap();
    let fresh = b
        .activate_with_parameters(
            &scope,
            None,
            unix_time_millis() + 60000,
            false,
            Some(b"workspace=w-new&model_url=http%3A%2F%2Fnew.example"),
        )
        .await
        .unwrap();
    assert_ne!(fresh.binding.generation, first.binding.generation);
}

#[tokio::test]
async fn invalid_initialization_has_no_binding_or_environment_side_effect() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Capture::default());
    let a = Activator::new(state.clone(), platform.clone());
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&template()))
        .await
        .unwrap();
    for input in [
        None,
        Some(b"workspace=../bad&model_url=x".as_slice()),
        Some(b"workspace=a&workspace=b&model_url=x".as_slice()),
        Some(b"workspace=w&model_url=%GG".as_slice()),
    ] {
        assert!(a
            .activate_with_parameters(&scope(), None, unix_time_millis() + 60000, false, input)
            .await
            .is_err());
        assert!(state.binding(&scope()).await.unwrap().is_none());
        assert!(platform.0.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn concurrent_first_access_uses_the_winning_parameters() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Capture::default());
    let a = Activator::new(state.clone(), platform.clone());
    let b = Activator::new(state.clone(), platform.clone());
    a.publish("tenant", &adx_agent_core::TemplateDraft::from(&template()))
        .await
        .unwrap();
    let scope = scope();
    let deadline = unix_time_millis() + 60000;
    let (left, right) = tokio::join!(
        a.activate_with_parameters(
            &scope,
            None,
            deadline,
            false,
            Some(b"workspace=left&model_url=http%3A%2F%2Fleft")
        ),
        b.activate_with_parameters(
            &scope,
            None,
            deadline,
            false,
            Some(b"workspace=right&model_url=http%3A%2F%2Fright")
        )
    );
    assert_eq!(left.unwrap().binding, right.unwrap().binding);
    let requests = platform.0.lock().unwrap();
    assert!(requests.windows(2).all(|pair| pair[0] == pair[1]));
}
