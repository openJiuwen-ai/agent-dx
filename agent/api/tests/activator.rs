use adx_activator::Activator;
use adx_agent_api::{activator::ActivatorClient, managed::ManagedService, Error};
use adx_agent_core::{environment::*, *};
use adx_agent_store::{AgentState, MemoryRepository};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
fn context() -> adx_agent_api::request::RequestContext {
    adx_agent_api::request::RequestContext::new(Duration::from_secs(3))
}
const TOKEN: &str = "test-activator-token-at-least-32-bytes";
struct Backend(AtomicUsize);
#[async_trait::async_trait]
impl Environment for Backend {
    async fn create(
        &self,
        r: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(EnvironmentObservation {
            id: r.id.clone(),
            tenant: r.tenant.clone(),
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
    async fn delete(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        Ok(EnvironmentObservation {
            id: id.into(),
            tenant: tenant.into(),
            phase: EnvironmentPhase::Deleted,
            ready: false,
            runtime_id: None,
            message: None,
        })
    }
}
#[tokio::test]
async fn authenticated_services_observe_binding_deletion_and_recreation() {
    let backend = Arc::new(Backend(AtomicUsize::new(0)));
    let activator = Arc::new(Activator::new(
        AgentState::new(Arc::new(MemoryRepository::default())),
        backend.clone(),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let template_calls = Arc::new(AtomicUsize::new(0));
    let activate_calls = Arc::new(AtomicUsize::new(0));
    let tc = template_calls.clone();
    let ac = activate_calls.clone();
    let app = adx_activator::server::router(activator, TOKEN, Duration::from_secs(3))
        .unwrap()
        .layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let tc = tc.clone();
                let ac = ac.clone();
                async move {
                    if request.uri().path().ends_with("templates/get") {
                        tc.fetch_add(1, Ordering::SeqCst);
                    }
                    if matches!(
                        request.uri().path(),
                        "/internal/adx/v1/bindings/activate" | "/internal/adx/v1/bindings/resolve"
                    ) {
                        ac.fetch_add(1, Ordering::SeqCst);
                    }
                    next.run(request).await
                }
            },
        ));
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let down = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    let client = Arc::new(
        ActivatorClient::new(
            vec![down, url.clone()],
            TOKEN.into(),
            Duration::from_secs(2),
            None,
            true,
        )
        .unwrap(),
    );
    let service = ManagedService::new(client.clone());
    let other_client = Arc::new(
        ActivatorClient::new(
            vec![url.clone()],
            TOKEN.into(),
            Duration::from_secs(2),
            None,
            true,
        )
        .unwrap(),
    );
    let other_gateway = ManagedService::new(other_client.clone());
    let template:TemplateVersion=serde_json::from_value(serde_json::json!({"name":"app","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080}]})).unwrap();
    other_gateway
        .publish(
            &context(),
            "tenant",
            &adx_agent_core::TemplateDraft::from(&template),
        )
        .await
        .unwrap();
    service
        .publish(
            &context(),
            "tenant",
            &adx_agent_core::TemplateDraft::from(&template),
        )
        .await
        .unwrap();
    let scope = Scope {
        tenant: "tenant".into(),
        template: "app".into(),
        template_ref: adx_agent_core::TemplateRef::Version("1".into()),
        binding_id: "env".into(),
    };
    assert!(matches!(
        service
            .resolve(&context(), &scope, Protocol::Ssh, None)
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(backend.0.load(Ordering::SeqCst), 0);
    assert!(matches!(
        service.binding(&context(), &scope).await,
        Err(Error::NotFound)
    ));
    let first = service
        .resolve(&context(), &scope, Protocol::Http, None)
        .await
        .unwrap();
    template_calls.store(0, Ordering::SeqCst);
    activate_calls.store(0, Ordering::SeqCst);
    assert_eq!(
        service
            .resolve(&context(), &scope, Protocol::Http, None)
            .await
            .unwrap(),
        first
    );
    assert_eq!(
        template_calls.load(Ordering::SeqCst),
        0,
        "warm Gateway must reuse its template"
    );
    assert_eq!(
        activate_calls.load(Ordering::SeqCst),
        1,
        "warm resolve is exactly one Activator RPC"
    );
    assert_eq!(
        other_client
            .activate(&context(), &scope, None)
            .await
            .unwrap()
            .binding,
        first.0.binding
    );
    let page = other_gateway
        .list_bindings(
            &context(),
            &activator::BindingList {
                tenant: "tenant".into(),
                template: "app".into(),
                template_ref: adx_agent_core::TemplateRef::Version("1".into()),
                page_size: 10,
                page_token: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.bindings, vec![first.0.binding.clone()]);
    let env = service.binding(&context(), &scope).await.unwrap();
    let other_target = other_gateway
        .resolve(&context(), &scope, Protocol::Http, None)
        .await
        .unwrap();
    assert_eq!(first, other_target);
    assert_eq!(first.0.binding, env);
    other_gateway
        .delete_binding(&context(), &scope)
        .await
        .unwrap();
    let fresh = service
        .resolve(&context(), &scope, Protocol::Http, None)
        .await
        .unwrap()
        .0
        .binding;
    assert_ne!(fresh.generation, env.generation);
    assert_ne!(fresh.current.environment_id, env.current.environment_id);
    assert!(matches!(
        other_client
            .activate(&context(), &scope, Some(&env.generation))
            .await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(
        service
            .resolve(&context(), &scope, Protocol::Http, None)
            .await
            .unwrap()
            .0
            .binding,
        fresh
    );
    let response = reqwest::Client::new()
        .post(format!("{url}/internal/adx/v1/bindings/get"))
        .json(&serde_json::json!({"scope":scope}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 401);
    let response = reqwest::Client::new()
        .post(format!("{url}/internal/adx/v1/bindings/get"))
        .bearer_auth(TOKEN)
        .header(activator::DEADLINE_HEADER, "1")
        .json(&serde_json::json!({"scope":scope}))
        .send()
        .await
        .unwrap();
    assert!(matches!(
        response.json::<Error>().await.unwrap(),
        Error::Unavailable(_)
    ));
    task.abort();
}

#[tokio::test]
async fn initialization_crosses_authenticated_activation_api() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let activator = Arc::new(Activator::new(
        state,
        Arc::new(Backend(AtomicUsize::new(0))),
    ));
    let template:TemplateVersion=serde_json::from_value(serde_json::json!({"name":"app","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080}]})).unwrap();
    activator
        .publish("tenant", &adx_agent_core::TemplateDraft::from(&template))
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = adx_activator::server::router(activator, TOKEN, Duration::from_secs(3)).unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let scope = Scope {
        tenant: "tenant".into(),
        template: "app".into(),
        template_ref: adx_agent_core::TemplateRef::Version("1".into()),
        binding_id: "private".into(),
    };
    let parameters = adx_agent_core::parameters::encode(&std::collections::BTreeMap::from([(
        "workspace".into(),
        "w-user".into(),
    )]));
    let response = reqwest::Client::new()
        .post(format!("{url}/internal/adx/v1/bindings/activate"))
        .json(&adx_agent_core::activator::ActivationRequest {
            scope: scope.clone(),
            expected_generation: None,
            bypass_cache: false,
            parameters: Some(parameters.clone()),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 401);
    let client =
        ActivatorClient::new(vec![url], TOKEN.into(), Duration::from_secs(3), None, true).unwrap();
    let binding = client
        .activate_with_parameters(&context(), &scope, None, false, Some(&parameters))
        .await
        .unwrap()
        .binding;
    assert_eq!(binding.parameters["workspace"], "w-user");
    assert_eq!(client.binding(&context(), &scope).await.unwrap(), binding);
    assert_eq!(
        client
            .activate_with_parameters(&context(), &scope, None, true, Some(b"%invalid"))
            .await
            .unwrap()
            .binding,
        binding
    );
    server.abort();
}
