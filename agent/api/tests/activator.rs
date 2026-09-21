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
    let app = adx_activator::server::router(activator, TOKEN, Duration::from_secs(3)).unwrap();
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
    let other_gateway = ManagedService::new(client.clone());
    let template:TemplateVersion=serde_json::from_value(serde_json::json!({"name":"app","version":"1","image":"app:1","isolation_runtime":"runc","entrypoint":["/start"],"resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080}]})).unwrap();
    service.publish("tenant", &template).await.unwrap();
    let scope = Scope {
        tenant: "tenant".into(),
        template: "app".into(),
        version: "1".into(),
        environment_id: "env".into(),
    };
    let env = service.create_environment(&scope).await.unwrap();
    assert!(matches!(
        service.resolve(&scope, Protocol::Ssh, None).await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(backend.0.load(Ordering::SeqCst), 0);
    let first = service.resolve(&scope, Protocol::Http, None).await.unwrap();
    let other_target = other_gateway
        .resolve(&scope, Protocol::Http, None)
        .await
        .unwrap();
    assert_eq!(first, other_target);
    assert_eq!(first.0.environment, env);
    other_gateway.delete_environment(&scope).await.unwrap();
    assert!(matches!(
        service.resolve(&scope, Protocol::Http, None).await,
        Err(Error::NotFound)
    ));
    let fresh = other_gateway.create_environment(&scope).await.unwrap();
    assert_ne!(fresh.generation, env.generation);
    assert_ne!(fresh.sandbox_id, env.sandbox_id);
    assert_eq!(
        service
            .resolve(&scope, Protocol::Http, None)
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
