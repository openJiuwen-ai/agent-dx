use adx_activator::Activator;
use adx_agent_api::{activator::ActivatorClient, Error};
use adx_agent_core::{sandbox::*, Scope, TemplateVersion};
use adx_agent_store::{AgentState, Index, Key, MemoryRepository, Record, Repository, Transaction};
use std::{sync::Arc, time::Duration};

struct StalledStore;
#[async_trait::async_trait]
impl Repository for StalledStore {
    async fn get(&self, _: &Key) -> adx_agent_store::Result<Option<Record>> {
        std::future::pending().await
    }
    async fn commit(&self, _: &Transaction) -> adx_agent_store::Result<bool> {
        std::future::pending().await
    }
    async fn page(
        &self,
        _: &Index,
        _: Option<&str>,
        _: usize,
    ) -> adx_agent_store::Result<Vec<(String, Record)>> {
        std::future::pending().await
    }
}
struct StalledSandbox;
#[async_trait::async_trait]
impl Sandbox for StalledSandbox {
    async fn create(&self, _: &CreateSandbox) -> Result<SandboxObservation, SandboxError> {
        std::future::pending().await
    }
    async fn get(&self, _: &str, _: &str) -> Result<Option<SandboxObservation>, SandboxError> {
        std::future::pending().await
    }
    async fn delete(&self, _: &str, _: &str) -> Result<SandboxObservation, SandboxError> {
        std::future::pending().await
    }
}
fn scope() -> Scope {
    Scope {
        tenant: "tenant".into(),
        template: "app".into(),
        version: "1".into(),
        environment_id: "env".into(),
    }
}
fn template() -> TemplateVersion {
    serde_json::from_value(serde_json::json!({"name":"app","version":"1","image":"app:1","isolation_runtime":"runc","entrypoint":["/start"],"resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080}]})).unwrap()
}

fn context() -> adx_agent_api::request::RequestContext {
    adx_agent_api::request::RequestContext::new(Duration::from_secs(1))
}
#[tokio::test]
async fn caller_deadline_distinguishes_remote_reads_and_possible_writes() {
    let control = remote_control(Activator::new(
        AgentState::new(Arc::new(StalledStore)),
        Arc::new(StalledSandbox),
    ))
    .await;
    let ctx = context();
    assert!(matches!(
        ctx.run(control.template(&ctx, "tenant", "app", "1")).await,
        Err(Error::Unavailable(_))
    ));
    let ctx = context();
    assert!(matches!(
        ctx.run(control.environment(&ctx, &scope())).await,
        Err(Error::Unavailable(_))
    ));
    let ctx = context();
    assert!(matches!(
        ctx.run(control.publish(&ctx, "tenant", &template())).await,
        Err(Error::OutcomeUnknown(_))
    ));
}
#[tokio::test]
async fn timed_out_activation_keeps_identity_for_another_replica() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let a = remote_control(Activator::new(state.clone(), Arc::new(StalledSandbox))).await;
    let b = remote_control(Activator::new(state, Arc::new(StalledSandbox))).await;
    a.publish(&context(), "tenant", &template()).await.unwrap();
    let ctx = context();
    assert!(matches!(
        ctx.run(a.activate(&ctx, &scope(), None)).await,
        Err(Error::OutcomeUnknown(_))
    ));
    let original = b.environment(&context(), &scope()).await.unwrap();
    let ctx = context();
    assert!(matches!(
        ctx.run(b.activate(&ctx, &scope(), None)).await,
        Err(Error::OutcomeUnknown(_))
    ));
    assert_eq!(a.environment(&context(), &scope()).await.unwrap(), original);
    let ctx = context();
    assert!(matches!(
        ctx.run(a.delete_environment(&ctx, &scope())).await,
        Err(Error::OutcomeUnknown(_))
    ));
    assert_eq!(
        b.environment(&context(), &scope()).await.unwrap().phase,
        adx_agent_core::EnvironmentPhase::Deleting
    );
}

async fn remote_control(activator: Activator) -> ActivatorClient {
    let token = "deadline-test-service-token-at-least-32-bytes";
    let timeout = Duration::from_secs(3);
    let app = adx_activator::server::router(Arc::new(activator), token, timeout).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    ActivatorClient::new(vec![url], token.into(), timeout, None, true).unwrap()
}

#[tokio::test]
async fn remote_activation_passes_the_original_deadline_to_sandbox_create() {
    struct ObserveDeadline(std::sync::Mutex<Option<u64>>);
    #[async_trait::async_trait]
    impl Sandbox for ObserveDeadline {
        async fn create(
            &self,
            request: &CreateSandbox,
        ) -> Result<SandboxObservation, SandboxError> {
            *self.0.lock().unwrap() = request.deadline_unix_ms;
            Ok(SandboxObservation {
                id: request.id.clone(),
                tenant: request.tenant.clone(),
                phase: SandboxPhase::Running,
                ready: true,
                runtime_id: None,
                message: None,
            })
        }
        async fn get(&self, _: &str, _: &str) -> Result<Option<SandboxObservation>, SandboxError> {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(None)
        }
        async fn delete(&self, _: &str, _: &str) -> Result<SandboxObservation, SandboxError> {
            Err(SandboxError::NotFound)
        }
    }
    let backend = Arc::new(ObserveDeadline(std::sync::Mutex::new(None)));
    let client = remote_control(Activator::new(
        AgentState::new(Arc::new(MemoryRepository::default())),
        backend.clone(),
    ))
    .await;
    client
        .publish(&context(), "tenant", &template())
        .await
        .unwrap();
    let ctx = context();
    let original = ctx.deadline_unix_ms();
    let target = client.activate(&ctx, &scope(), None).await.unwrap();
    assert_eq!(target.environment.scope, scope());
    let received = backend.0.lock().unwrap().unwrap();
    assert!(
        received <= original + 2,
        "deadline was extended: {received} > {original}"
    );
    assert!(received >= original.saturating_sub(10));
}
