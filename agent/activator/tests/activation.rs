use adx_activator::{Activator, Error};
use adx_agent_core::{sandbox::*, *};
use adx_agent_store::{AgentState, MemoryRepository};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

#[derive(Default)]
struct Platform {
    observations: Mutex<HashMap<(String, String), SandboxObservation>>,
    lose_create_response: AtomicBool,
    lose_delete_response: AtomicBool,
}
#[async_trait::async_trait]
impl Sandbox for Platform {
    async fn create(&self, r: &CreateSandbox) -> Result<SandboxObservation, SandboxError> {
        let observed = self
            .observations
            .lock()
            .unwrap()
            .entry((r.tenant.clone(), r.id.clone()))
            .or_insert_with(|| SandboxObservation {
                id: r.id.clone(),
                tenant: r.tenant.clone(),
                phase: SandboxPhase::Running,
                ready: true,
                runtime_id: Some("runtime".into()),
                message: None,
            })
            .clone();
        if self.lose_create_response.swap(false, Ordering::SeqCst) {
            return Err(SandboxError::OutcomeUnknown("response lost".into()));
        }
        Ok(observed)
    }
    async fn get(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<SandboxObservation>, SandboxError> {
        Ok(self
            .observations
            .lock()
            .unwrap()
            .get(&(tenant.into(), id.into()))
            .cloned())
    }
    async fn delete(&self, tenant: &str, id: &str) -> Result<SandboxObservation, SandboxError> {
        if self.lose_delete_response.swap(false, Ordering::SeqCst) {
            return Err(SandboxError::OutcomeUnknown("delete response lost".into()));
        }
        let mut values = self.observations.lock().unwrap();
        let value = values
            .get_mut(&(tenant.into(), id.into()))
            .ok_or(SandboxError::NotFound)?;
        value.phase = SandboxPhase::Deleted;
        value.ready = false;
        Ok(value.clone())
    }
}
fn template() -> TemplateVersion {
    serde_json::from_value(serde_json::json!({"name":"app","version":"1","image":"app:1","isolation_runtime":"runc","entrypoint":["/start"],"resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080}]})).unwrap()
}
fn scope(id: &str) -> Scope {
    Scope {
        tenant: "tenant".into(),
        template: "app".into(),
        version: "1".into(),
        environment_id: id.into(),
    }
}
#[tokio::test]
async fn replicas_and_unknown_outcomes_keep_the_committed_identity() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let a = Activator::new(state.clone(), platform.clone());
    let b = Activator::new(state.clone(), platform.clone());
    a.publish("tenant", &template()).await.unwrap();
    let original = a.create_environment(scope("e")).await.unwrap();
    platform.lose_create_response.store(true, Ordering::SeqCst);
    assert!(matches!(
        a.activate(&scope("e")).await,
        Err(Error::OutcomeUnknown(_))
    ));
    let e = scope("e");
    let (x, y) = tokio::join!(a.activate(&e), b.activate(&e));
    assert_eq!(x.unwrap(), y.unwrap());
    assert_eq!(b.environment(&scope("e")).await.unwrap(), original);
    assert_eq!(platform.observations.lock().unwrap().len(), 1);
    platform.lose_delete_response.store(true, Ordering::SeqCst);
    assert!(matches!(
        b.delete_environment(&scope("e")).await,
        Err(Error::OutcomeUnknown(_))
    ));
    assert!(matches!(
        a.activate(&scope("e")).await,
        Err(Error::Conflict(_))
    ));
    a.delete_environment(&scope("e")).await.unwrap();
    let fresh = b.create_environment(scope("e")).await.unwrap();
    assert_ne!(fresh.sandbox_id, original.sandbox_id);
    assert_eq!(a.activate(&scope("e")).await.unwrap().environment, fresh);
}
#[tokio::test]
async fn independent_environments_and_platform_readiness() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let service = Activator::new(state.clone(), platform.clone());
    service.publish("tenant", &template()).await.unwrap();
    let (x, y) = tokio::join!(
        service.create_environment(scope("a")),
        service.create_environment(scope("b"))
    );
    let x = x.unwrap();
    let y = y.unwrap();
    assert_ne!(x.sandbox_id, y.sandbox_id);
    let sa = scope("a");
    let sb = scope("b");
    let (a, b) = tokio::join!(service.activate(&sa), service.activate(&sb));
    a.unwrap();
    b.unwrap();
    platform
        .observations
        .lock()
        .unwrap()
        .get_mut(&(x.scope.tenant.clone(), x.sandbox_id.clone()))
        .unwrap()
        .ready = false;
    assert!(matches!(
        service.activate(&scope("a")).await,
        Err(Error::NotReady(_))
    ));
    assert_eq!(service.environment(&scope("a")).await.unwrap(), x);
    let other = Scope {
        tenant: "other".into(),
        ..scope("a")
    };
    assert!(matches!(
        service.activate(&other).await,
        Err(Error::NotFound)
    ));
}
