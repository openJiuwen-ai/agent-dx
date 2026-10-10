use adx_activator::local_sandbox::{LocalSandbox, SandboxApplication};
use adx_agent_core::sandbox::*;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct Application {
    creates: Mutex<Vec<(String, Value, String)>>,
    stall: bool,
}
#[async_trait]
impl SandboxApplication for Application {
    async fn create_request(
        &self,
        tenant: &str,
        input: Value,
        operation_id: &str,
    ) -> Result<Value, SandboxError> {
        self.creates
            .lock()
            .unwrap()
            .push((tenant.into(), input, operation_id.into()));
        if self.stall {
            std::future::pending().await
        } else {
            Ok(json!({"sandboxId":"adx-id","instanceId":"adx-id","status":"running"}))
        }
    }
    async fn instance(&self, _: &str, id: &str) -> Result<Option<Value>, SandboxError> {
        Ok(Some(json!({"id":id,"status":"running"})))
    }
    async fn delete_request(
        &self,
        _: &str,
        _: &str,
        _: Value,
        _: &str,
    ) -> Result<(), SandboxError> {
        Err(SandboxError::OutcomeUnknown("not confirmed".into()))
    }
}
fn request() -> CreateSandbox {
    serde_json::from_value(json!({"tenant":"tenant","id":"adx-id","execution":{
        "image":"app:1","isolation_runtime":"runc","inherit_entrypoint":true,"entrypoint":[],"working_dir":"","user":null,"env":{"MODEL_KEY":"test-key"},"resources":{"cpu_millis":1000,"memory_mib":512},"service":[]
    }})).unwrap()
}
#[tokio::test]
async fn local_adapter_passes_the_public_request_and_preserves_unknown_delete() {
    let app = Arc::new(Application::default());
    let sandbox = LocalSandbox::new(app.clone(), Duration::from_secs(60)).unwrap();
    assert!(sandbox.create(&request()).await.unwrap().ready);
    let calls = app.creates.lock().unwrap().clone();
    assert_eq!(calls[0].0, "tenant");
    assert_eq!(
        calls[0].1,
        json!({"namespace":"adx","name":"id","image":"app:1","runtime":"runc","inheritEntrypoint":true,"env":{"MODEL_KEY":"test-key"},"cpu":1000,"memory":512,"createTimeoutSeconds":60})
    );
    assert!(calls[0].2.starts_with("activator-create-"));
    assert!(
        sandbox
            .get("tenant", "adx-id")
            .await
            .unwrap()
            .unwrap()
            .ready
    );
    assert!(matches!(
        sandbox.delete("tenant", "adx-id").await,
        Err(SandboxError::OutcomeUnknown(_))
    ));
}
#[tokio::test]
async fn expired_calls_are_not_submitted_and_submitted_timeouts_remain_unknown() {
    let app = Arc::new(Application {
        stall: true,
        ..Default::default()
    });
    let sandbox = LocalSandbox::new(app.clone(), Duration::from_secs(60)).unwrap();
    let mut request = request();
    request.deadline_unix_ms = Some(adx_agent_core::unix_time_millis().saturating_sub(1));
    assert!(matches!(
        sandbox.create(&request).await,
        Err(SandboxError::Unavailable(_))
    ));
    assert!(app.creates.lock().unwrap().is_empty());
    request.deadline_unix_ms = Some(adx_agent_core::unix_time_millis() + 50);
    assert!(matches!(
        sandbox.create(&request).await,
        Err(SandboxError::OutcomeUnknown(_))
    ));
    assert_eq!(app.creates.lock().unwrap().len(), 1);
}
