use adx_activator::Activator;
use adx_agent_api::{activator::ActivatorClient, managed::ManagedService, Error};
use adx_agent_core::{sandbox::*, *};
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
impl Sandbox for Backend {
    async fn create(&self, r: &CreateSandbox) -> Result<SandboxObservation, SandboxError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(SandboxObservation {
            id: r.id.clone(),
            tenant: r.tenant.clone(),
            phase: SandboxPhase::Running,
            ready: true,
            runtime_id: None,
            message: None,
        })
    }
    async fn get(&self, _: &str, _: &str) -> Result<Option<SandboxObservation>, SandboxError> {
        Ok(None)
    }
    async fn delete(&self, tenant: &str, id: &str) -> Result<SandboxObservation, SandboxError> {
        Ok(SandboxObservation {
            id: id.into(),
            tenant: tenant.into(),
            phase: SandboxPhase::Deleted,
            ready: false,
            runtime_id: None,
            message: None,
        })
    }
}
#[tokio::test]
async fn authenticated_services_observe_environment_deletion_and_recreation() {
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
                    if request.uri().path().ends_with("environments/activate") {
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
    let template:TemplateVersion=serde_json::from_value(serde_json::json!({"name":"app","version":"1","image":"app:1","isolation_runtime":"runc","entrypoint":["/start"],"resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080}]})).unwrap();
    other_gateway
        .publish(&context(), "tenant", &template)
        .await
        .unwrap();
    service
        .publish(&context(), "tenant", &template)
        .await
        .unwrap();
    let scope = Scope {
        tenant: "tenant".into(),
        template: "app".into(),
        version: "1".into(),
        environment_id: "env".into(),
    };
    assert!(matches!(
        service
            .resolve(&context(), &scope, Protocol::Ssh, None)
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(backend.0.load(Ordering::SeqCst), 0);
    assert!(matches!(
        service.environment(&context(), &scope).await,
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
            .environment,
        first.0.environment
    );
    let page = other_gateway
        .list_environments(
            &context(),
            &activator::EnvironmentList {
                tenant: "tenant".into(),
                template: "app".into(),
                version: "1".into(),
                page_size: 10,
                page_token: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.environments, vec![first.0.environment.clone()]);
    let env = service.environment(&context(), &scope).await.unwrap();
    let other_target = other_gateway
        .resolve(&context(), &scope, Protocol::Http, None)
        .await
        .unwrap();
    assert_eq!(first, other_target);
    assert_eq!(first.0.environment, env);
    other_gateway
        .delete_environment(&context(), &scope)
        .await
        .unwrap();
    let fresh = service
        .resolve(&context(), &scope, Protocol::Http, None)
        .await
        .unwrap()
        .0
        .environment;
    assert_ne!(fresh.generation, env.generation);
    assert_ne!(fresh.sandbox_id, env.sandbox_id);
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
            .environment,
        fresh
    );
    let response = reqwest::Client::new()
        .post(format!("{url}/internal/adx/v1/environments/get"))
        .json(&serde_json::json!({"scope":scope}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 401);
    let response = reqwest::Client::new()
        .post(format!("{url}/internal/adx/v1/environments/get"))
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
