use adx_activator::Activator;
use adx_agent_api::{activator::Control, local::LocalControl, Error};
use adx_agent_core::{environment::*, Scope, TemplateVersion};
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
struct StalledEnvironment;
#[async_trait::async_trait]
impl Environment for StalledEnvironment {
    async fn create(
        &self,
        _: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        std::future::pending().await
    }
    async fn get(
        &self,
        _: &str,
        _: &str,
    ) -> Result<Option<EnvironmentObservation>, EnvironmentError> {
        std::future::pending().await
    }
    async fn delete(&self, _: &str, _: &str) -> Result<EnvironmentObservation, EnvironmentError> {
        std::future::pending().await
    }
}
fn scope() -> Scope {
    Scope {
        tenant: "tenant".into(),
        template: "app".into(),
        version: "1".into(),
        binding_id: "env".into(),
    }
}
fn template() -> TemplateVersion {
    serde_json::from_value(serde_json::json!({"name":"app","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080}]})).unwrap()
}

fn context() -> adx_agent_api::request::RequestContext {
    adx_agent_api::request::RequestContext::new(Duration::from_secs(1))
}
#[tokio::test(start_paused = true)]
async fn caller_deadline_distinguishes_local_reads_and_possible_writes() {
    let control = LocalControl::new(Arc::new(Activator::new(
        AgentState::new(Arc::new(StalledStore)),
        Arc::new(StalledEnvironment),
    )));
    let ctx = context();
    assert!(matches!(
        ctx.run(control.template(&ctx, "tenant", "app", "1")).await,
        Err(Error::Unavailable(_))
    ));
    let ctx = context();
    assert!(matches!(
        ctx.run(control.binding(&ctx, &scope())).await,
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
        Arc::new(StalledEnvironment),
    )));
    let b = LocalControl::new(Arc::new(Activator::new(
        state,
        Arc::new(StalledEnvironment),
    )));
    a.publish(&context(), "tenant", &template()).await.unwrap();
    let ctx = context();
    assert!(matches!(
        ctx.run(a.activate(&ctx, &scope(), None)).await,
        Err(Error::OutcomeUnknown(_))
    ));
    let original = b.binding(&context(), &scope()).await.unwrap();
    let ctx = context();
    assert!(matches!(
        ctx.run(b.activate(&ctx, &scope(), None)).await,
        Err(Error::OutcomeUnknown(_))
    ));
    assert_eq!(a.binding(&context(), &scope()).await.unwrap(), original);
    let ctx = context();
    assert!(matches!(
        ctx.run(a.delete_binding(&ctx, &scope())).await,
        Err(Error::OutcomeUnknown(_))
    ));
    assert_eq!(
        b.binding(&context(), &scope()).await.unwrap().phase,
        adx_agent_core::BindingPhase::Deleting
    );
}

#[derive(Default)]
struct ReadyEnvironment {
    record: std::sync::Mutex<Option<EnvironmentObservation>>,
    unavailable: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl Environment for ReadyEnvironment {
    async fn create(
        &self,
        r: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        let observation = EnvironmentObservation {
            id: r.id.clone(),
            tenant: r.tenant.clone(),
            phase: EnvironmentPhase::Running,
            ready: true,
            runtime_id: None,
            message: None,
        };
        *self.record.lock().unwrap() = Some(observation.clone());
        Ok(observation)
    }
    async fn get(
        &self,
        _: &str,
        _: &str,
    ) -> Result<Option<EnvironmentObservation>, EnvironmentError> {
        if self.unavailable.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(EnvironmentError::Unavailable(
                "test backend unavailable".into(),
            ));
        }
        Ok(self.record.lock().unwrap().clone())
    }
    async fn delete(&self, _: &str, _: &str) -> Result<EnvironmentObservation, EnvironmentError> {
        Err(EnvironmentError::Unsupported("unused by this test".into()))
    }
}

#[tokio::test]
async fn embedded_resolve_bypass_invalidates_warm_binding_before_backend_failure() {
    use adx_agent_api::managed::ManagedService;
    use adx_agent_core::Protocol;
    use std::sync::atomic::Ordering;
    let backend = Arc::new(ReadyEnvironment::default());
    let control = Arc::new(LocalControl::new(Arc::new(Activator::new(
        AgentState::new(Arc::new(MemoryRepository::default())),
        backend.clone(),
    ))));
    control
        .publish(&context(), "tenant", &template())
        .await
        .unwrap();
    let managed = ManagedService::new(control);
    let original = managed
        .resolve(&context(), &scope(), Protocol::Http, None)
        .await
        .unwrap();
    backend.unavailable.store(true, Ordering::SeqCst);
    assert_eq!(
        managed
            .resolve(&context(), &scope(), Protocol::Http, None)
            .await
            .unwrap(),
        original
    );
    assert!(matches!(
        managed
            .resolve_with_cache(&context(), &scope(), Protocol::Http, None, true)
            .await,
        Err(Error::OutcomeUnknown(_))
    ));
    assert!(matches!(
        managed
            .resolve(&context(), &scope(), Protocol::Http, None)
            .await,
        Err(Error::OutcomeUnknown(_))
    ));
    backend.unavailable.store(false, Ordering::SeqCst);
    assert_eq!(
        managed
            .resolve(&context(), &scope(), Protocol::Http, None)
            .await
            .unwrap(),
        original
    );
}
