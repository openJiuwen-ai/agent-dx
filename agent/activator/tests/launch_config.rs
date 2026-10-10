use adx_activator::Activator;
use adx_agent_core::{launch::LaunchConfig, sandbox::*, *};
use adx_agent_store::{AgentState, MemoryRepository};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Capture(Mutex<Vec<CreateSandbox>>);
#[async_trait::async_trait]
impl Sandbox for Capture {
    async fn create(&self, request: &CreateSandbox) -> Result<SandboxObservation, SandboxError> {
        self.0.lock().unwrap().push(request.clone());
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
        Ok(None)
    }
    async fn delete(&self, _: &str, _: &str) -> Result<SandboxObservation, SandboxError> {
        Err(SandboxError::NotFound)
    }
}
#[tokio::test]
async fn private_configuration_is_frozen_and_reused_across_activators() {
    let store = Arc::new(MemoryRepository::default());
    let state = AgentState::new(store).with_credential_key([7; 32]);
    let platform = Arc::new(Capture::default());
    let a = Activator::new(state.clone(), platform.clone());
    let b = Activator::new(state, platform.clone());
    let template: TemplateVersion = serde_json::from_value(serde_json::json!({"name":"app","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"ws","port":8080}],"env":{"API_KEY":"shared","MODEL_NAME":"model"}})).unwrap();
    a.publish("tenant", &template).await.unwrap();
    let scope = Scope {
        tenant: "tenant".into(),
        template: "app".into(),
        version: "1".into(),
        binding_id: "alice".into(),
    };
    let launch = LaunchConfig {
        credential_version: "v1".into(),
        env: BTreeMap::from([("API_KEY".into(), "sk-alice-private".into())]),
    };
    let binding = a.prepare_binding(&scope, &launch).await.unwrap();
    assert_eq!(b.prepare_binding(&scope, &launch).await.unwrap(), binding);
    let mut changed = launch.clone();
    changed
        .env
        .insert("API_KEY".into(), "sk-bob-private".into());
    assert!(b.prepare_binding(&scope, &changed).await.is_err());
    assert!(!serde_json::to_string(&binding)
        .unwrap()
        .contains("sk-alice"));
    assert!(!format!("{launch:?}").contains("sk-alice"));
    let deadline = unix_time_millis() + 60000;
    a.activate(&scope, None, deadline).await.unwrap();
    b.activate(&scope, None, deadline).await.unwrap();
    let creates = platform.0.lock().unwrap();
    assert_eq!(creates.len(), 2);
    assert_eq!(creates[0], creates[1]);
    assert_eq!(creates[0].execution.env["API_KEY"], "sk-alice-private");
    assert_eq!(creates[0].execution.env["MODEL_NAME"], "model");
}
