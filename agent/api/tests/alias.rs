use adx_activator::Activator;
use adx_agent_api::{local::LocalControl, managed::ManagedService, request::RequestContext, Error};
use adx_agent_core::{
    alias::{AliasConfig, VersionWeight},
    environment::*,
    *,
};
use adx_agent_store::{AgentState, MemoryRepository};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct Platform(Mutex<BTreeMap<String, EnvironmentObservation>>);
#[async_trait::async_trait]
impl Environment for Platform {
    async fn get(
        &self,
        _: &str,
        id: &str,
    ) -> Result<Option<EnvironmentObservation>, EnvironmentError> {
        Ok(self.0.lock().unwrap().get(id).cloned())
    }
    async fn create(
        &self,
        r: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        let value = EnvironmentObservation {
            id: r.id.clone(),
            tenant: r.tenant.clone(),
            phase: EnvironmentPhase::Running,
            ready: true,
            runtime_id: None,
            message: None,
        };
        self.0.lock().unwrap().insert(r.id.clone(), value.clone());
        Ok(value)
    }
    async fn delete(&self, _: &str, id: &str) -> Result<EnvironmentObservation, EnvironmentError> {
        let mut value = self
            .0
            .lock()
            .unwrap()
            .remove(id)
            .ok_or(EnvironmentError::NotFound)?;
        value.phase = EnvironmentPhase::Deleted;
        Ok(value)
    }
}
#[tokio::test]
async fn alias_service_validation_uses_current_version_and_prewrite_retry_uses_new_default_port() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let activator = Arc::new(Activator::new(state.clone(), platform.clone()));
    let managed = ManagedService::new(Arc::new(LocalControl::new(activator.clone())));
    for (version, port) in [("1", 8080), ("2", 9090)] {
        let template=serde_json::from_value(serde_json::json!({"name":"agent","version":version,"image":version,"isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":port}]})).unwrap();
        activator
            .publish("tenant", &adx_agent_core::TemplateDraft::from(&template))
            .await
            .unwrap();
    }
    let mut config = AliasConfig {
        tenant: "tenant".into(),
        template: "agent".into(),
        alias: "production".into(),
        seed: "stable".into(),
        versions: vec![VersionWeight {
            version: "1".into(),
            weight: 1,
        }],
    };
    let revision = state.put_alias(&config, None).await.unwrap();
    let scope = Scope {
        tenant: "tenant".into(),
        template: "agent".into(),
        template_ref: TemplateRef::Alias("production".into()),
        binding_id: "workspace".into(),
    };
    let context = RequestContext::new(Duration::from_secs(5));
    assert!(matches!(
        managed.resolve(&context, &scope, Protocol::Ssh, None).await,
        Err(Error::Invalid(_))
    ));
    assert!(
        state.binding(&scope).await.unwrap().is_none(),
        "invalid service must not create metadata"
    );
    assert!(platform.0.lock().unwrap().is_empty());
    let (old, port) = managed
        .resolve(&context, &scope, Protocol::Http, None)
        .await
        .unwrap();
    assert_eq!(port, 8080);
    config.versions[0].version = "2".into();
    state.put_alias(&config, Some(&revision)).await.unwrap();
    let view = managed.binding_view(&context, &scope).await.unwrap();
    assert_eq!(
        view.template.version, "1",
        "metadata must follow current, not the new Alias goal"
    );
    assert_eq!(view.binding.unwrap().current, old.binding.current);
    platform
        .0
        .lock()
        .unwrap()
        .get_mut(&old.binding.current.environment_id)
        .unwrap()
        .phase = EnvironmentPhase::Paused;
    activator
        .reconcile_binding(&scope, context.deadline_unix_ms())
        .await
        .unwrap();
    let (new, port) = managed
        .retry_resolve(
            &context,
            &scope,
            Protocol::Http,
            None,
            &old.binding.generation,
        )
        .await
        .unwrap();
    assert_eq!(port, 9090);
    assert_eq!(new.binding.current.version, "2");
    assert_eq!(new.binding.generation, old.binding.generation);
    assert_ne!(
        new.binding.current.environment_id,
        old.binding.current.environment_id
    );
    assert!(
        managed
            .retry_resolve(
                &context,
                &scope,
                Protocol::Http,
                Some(8080),
                &old.binding.generation
            )
            .await
            .is_err(),
        "explicit port must remain a caller constraint"
    );
}

#[tokio::test]
async fn alias_hot_resolve_uses_one_activator_request() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let activator = Arc::new(Activator::new(state.clone(), Arc::new(Platform::default())));
    let draft = serde_json::from_value(serde_json::json!({"name":"agent","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080}]})).unwrap();
    activator.publish("tenant", &draft).await.unwrap();
    state.put_alias(&serde_json::from_value(serde_json::json!({"tenant":"tenant","template":"agent","alias":"production","seed":"stable","versions":[{"version":"1","weight":1}]})).unwrap(), None).await.unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed = calls.clone();
    let token = "alias-hot-path-test-token-at-least-32-bytes";
    let app = adx_activator::server::router(activator, token, Duration::from_secs(5))
        .unwrap()
        .layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let observed = observed.clone();
                async move {
                    observed
                        .lock()
                        .unwrap()
                        .push(request.uri().path().to_owned());
                    next.run(request).await
                }
            },
        ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = adx_agent_api::activator::ActivatorClient::new(
        vec![url],
        token.into(),
        Duration::from_secs(5),
        None,
        true,
    )
    .unwrap();
    let managed = ManagedService::new(Arc::new(client));
    let scope = Scope {
        tenant: "tenant".into(),
        template: "agent".into(),
        template_ref: TemplateRef::Alias("production".into()),
        binding_id: "workspace".into(),
    };
    let context = RequestContext::new(Duration::from_secs(5));
    let view = managed.binding_view(&context, &scope).await.unwrap();
    assert!(view.binding.is_none());
    assert_eq!(view.template.version, "1");
    assert!(
        state.binding(&scope).await.unwrap().is_none(),
        "metadata lookup must not create a Binding"
    );
    let first = managed
        .resolve(&context, &scope, Protocol::Http, None)
        .await
        .unwrap();
    calls.lock().unwrap().clear();
    assert_eq!(
        managed
            .resolve(&context, &scope, Protocol::Http, None)
            .await
            .unwrap(),
        first
    );
    assert_eq!(
        *calls.lock().unwrap(),
        vec!["/internal/adx/v1/bindings/resolve"]
    );
    server.abort();
}

struct RejectDelete(Arc<Platform>);
#[async_trait::async_trait]
impl Environment for RejectDelete {
    async fn get(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<EnvironmentObservation>, EnvironmentError> {
        self.0.get(tenant, id).await
    }
    async fn create(
        &self,
        request: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        self.0.create(request).await
    }
    async fn delete(&self, _: &str, _: &str) -> Result<EnvironmentObservation, EnvironmentError> {
        Err(EnvironmentError::Unavailable(
            "temporary deletion failure".into(),
        ))
    }
}

#[tokio::test]
async fn missing_candidate_template_retires_before_clearing_and_does_not_recreate() {
    use adx_agent_store::{Check, Key, Repository, Transaction};
    let repository = Arc::new(MemoryRepository::default());
    let state = AgentState::new(repository.clone());
    let platform = Arc::new(Platform::default());
    let activator = Activator::new(state.clone(), platform.clone());
    let draft = serde_json::from_value(serde_json::json!({"name":"agent","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512}})).unwrap();
    state.publish("tenant", &draft).await.unwrap();
    let candidate_template = state.publish("tenant", &draft).await.unwrap();
    let mut config:AliasConfig = serde_json::from_value(serde_json::json!({"tenant":"tenant","template":"agent","alias":"production","seed":"stable","versions":[{"version":"1","weight":1}]})).unwrap();
    let revision = state.put_alias(&config, None).await.unwrap();
    let scope = Scope {
        tenant: "tenant".into(),
        template: "agent".into(),
        template_ref: TemplateRef::Alias("production".into()),
        binding_id: "workspace".into(),
    };
    let original = activator
        .activate(&scope, None, unix_time_millis() + 5000)
        .await
        .unwrap()
        .binding;
    config.versions[0].version = "2".into();
    state.put_alias(&config, Some(&revision)).await.unwrap();
    let prepared = state.prepare_binding(&scope).await.unwrap();
    let candidate = prepared.secondary.as_ref().unwrap().environment.clone();
    platform
        .create(&CreateEnvironment {
            id: candidate.environment_id.clone(),
            tenant: "tenant".into(),
            execution: (&candidate_template).into(),
            deadline_unix_ms: None,
            creation_attempt: 0,
        })
        .await
        .unwrap();
    let key = Key::new("template", &["tenant", "agent", "2"]).unwrap();
    let record = repository.get(&key).await.unwrap().unwrap();
    repository
        .commit(
            &Transaction::with_deletes(
                vec![Check {
                    key: key.clone(),
                    expected: Some(record.revision),
                }],
                vec![],
                vec![key],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    // A new process has no immutable-template cache masking the damaged metadata.
    let recovered = AgentState::new(repository.clone());
    let blocked = Activator::new(recovered.clone(), Arc::new(RejectDelete(platform.clone())));
    assert!(blocked
        .reconcile_binding(&scope, unix_time_millis() + 5000)
        .await
        .is_err());
    let retained = recovered.binding(&scope).await.unwrap().unwrap();
    assert_eq!(retained.current, original.current);
    let secondary = retained.secondary.unwrap();
    assert_eq!(secondary.environment, candidate);
    assert_eq!(secondary.intent, SecondaryIntent::Retiring);
    assert!(platform
        .0
        .lock()
        .unwrap()
        .contains_key(&candidate.environment_id));
    let restarted = Activator::new(recovered.clone(), platform.clone());
    restarted
        .reconcile_binding(&scope, unix_time_millis() + 5000)
        .await
        .unwrap();
    assert!(recovered
        .binding(&scope)
        .await
        .unwrap()
        .unwrap()
        .secondary
        .is_none());
    assert!(!platform
        .0
        .lock()
        .unwrap()
        .contains_key(&candidate.environment_id));
    assert!(platform
        .0
        .lock()
        .unwrap()
        .contains_key(&original.current.environment_id));
    assert!(restarted
        .reconcile_binding(&scope, unix_time_millis() + 5000)
        .await
        .is_err());
    let latest = recovered.binding(&scope).await.unwrap().unwrap();
    assert_eq!(latest.current, original.current);
    assert!(latest.secondary.is_none());
}
