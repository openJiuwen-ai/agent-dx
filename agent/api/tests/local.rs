use adx_activator::Activator;
use adx_agent_api::{activator::Control, local::LocalControl, Error};
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
#[tokio::test(start_paused = true)]
async fn caller_deadline_distinguishes_local_reads_and_possible_writes() {
    let control = LocalControl::new(Arc::new(Activator::new(
        AgentState::new(Arc::new(StalledStore)),
        Arc::new(StalledSandbox),
    )));
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
#[tokio::test(start_paused = true)]
async fn timed_out_activation_keeps_identity_for_another_local_replica() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let a = LocalControl::new(Arc::new(Activator::new(
        state.clone(),
        Arc::new(StalledSandbox),
    )));
    let b = LocalControl::new(Arc::new(Activator::new(state, Arc::new(StalledSandbox))));
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
