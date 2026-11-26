use adx_activator::Activator;
use adx_agent_core::{environment::*, *};
use adx_agent_store::{AgentState, MemoryRepository};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Capture(Mutex<Vec<CreateEnvironment>>);
#[async_trait::async_trait]
impl Environment for Capture {
    async fn ensure_volume(
        &self,
        _: &str,
        _: &str,
        spec: &volumes::VolumeSpec,
    ) -> Result<volumes::VolumeRef, EnvironmentError> {
        Ok(volumes::VolumeRef {
            id: spec.id.clone(),
            uid: format!("uid-{}", spec.id),
        })
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

#[tokio::test]
async fn digitalmate_uses_cold_start_when_golden_is_disabled() {
    use adx_agent_core::activator::{ActivationRequest, ResolveRequest, ServiceSelector};

    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Capture::default());
    let service = Activator::new(state, platform.clone());
    let digest = format!("sha256:{}", "a".repeat(64));
    let draft: TemplateDraft = serde_json::from_value(serde_json::json!({
        "name":"digitalmate","image":format!("product@{digest}"),"isolation_runtime":"runsc",
        "resources":{"cpu_millis":1000,"memory_mib":512},
        "service":[{"protocol":"ws","port":18092}],
        "env":{"DIGITALMATE_CRYPTO_WORKSPACE":"${workspace}"},
        "mounts":[{"type":"bind","target":"/data/state","volume_id":"user-${workspace}"}],
        "volumes":[{"id":"user-${workspace}","access_mode":"exclusive",
            "source":{"type":"directory","storage_domain":"sfs","path":"root/${workspace}"}}]
    }))
    .unwrap();
    let template = service.publish("tenant", &draft).await.unwrap();
    let request = ResolveRequest {
        activation: ActivationRequest {
            scope: Scope {
                tenant: "tenant".into(),
                template: template.name,
                template_ref: TemplateRef::Version(template.version),
                binding_id: "fixture".into(),
            },
            expected_generation: None,
            bypass_cache: false,
            parameters: Some(format!("workspace=w-{}", "a".repeat(64)).into_bytes()),
            reuse_subject_binding: true,
        },
        service: ServiceSelector {
            protocol: Protocol::Ws,
            port: Some(18092),
        },
    };
    let binding = service
        .resolve(&request, unix_time_millis() + 30_000)
        .await
        .unwrap()
        .target
        .binding;
    assert!(binding.current.snapshot_id.is_none());
    let requests = platform.0.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].execution.snapshot_id.is_none());
}

#[tokio::test]
async fn allocation_from_the_normal_entry_tracks_a_digitalmate_subject_across_scopes() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let service = Activator::new(state.clone(), Arc::new(Capture::default()));
    let draft = TemplateDraft::from(&template());
    service.publish("tenant", &draft).await.unwrap();
    service.publish("tenant", &draft).await.unwrap();
    let first = Scope {
        binding_id: format!("digitalmate-{}", "1".repeat(64)),
        ..scope()
    };
    let params = [
        ("workspace".into(), "w".into()),
        ("model_url".into(), "https://model.example".into()),
    ]
    .into();
    let assigned = state
        .create_binding_with_parameters(first.clone(), &params)
        .await
        .unwrap();
    let next = Scope {
        template_ref: TemplateRef::Version("2".into()),
        ..first.clone()
    };
    let (same, created) = state
        .create_binding_for_version(next.clone(), &params, true, "2", None)
        .await
        .unwrap();
    assert!(!created, "CAS loser must not report a cold binding");
    assert_eq!(same, assigned);
    service.delete_binding(&first).await.unwrap();
    assert_eq!(state.subject_binding(&next).await.unwrap(), None);
    let (rebound, created) = state
        .create_binding_for_version(next.clone(), &params, true, "2", None)
        .await
        .unwrap();
    assert!(created, "new binding must report a cold binding");
    assert_eq!(rebound.scope, next);
    assert_ne!(
        rebound.current.environment_id,
        assigned.current.environment_id
    );
    assert_eq!(state.subject_binding(&first).await.unwrap(), Some(rebound));
}
