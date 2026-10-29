mod support;

use adx_activator::local_environment::{EnvironmentApplication, LocalEnvironment};
use adx_agent_core::environment::*;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use support::network_policy;

#[derive(Default)]
struct Application {
    creates: Mutex<Vec<(String, Value, String)>>,
    stall: bool,
    absent: bool,
    state: Option<&'static str>,
}
#[async_trait]
impl EnvironmentApplication for Application {
    async fn create_request(
        &self,
        tenant: &str,
        input: Value,
        operation_id: &str,
    ) -> Result<Value, EnvironmentError> {
        self.creates
            .lock()
            .unwrap()
            .push((tenant.into(), input, operation_id.into()));
        if self.stall {
            std::future::pending().await
        } else {
            Ok(json!({"environmentId":"adx-id","status":"running"}))
        }
    }
    async fn instance(&self, _: &str, id: &str) -> Result<Option<Value>, EnvironmentError> {
        Ok(Some(
            json!({"id":id,"status":self.state.unwrap_or("running")}),
        ))
    }
    async fn delete_request(
        &self,
        _: &str,
        _: &str,
        _: Value,
        _: &str,
    ) -> Result<(), EnvironmentError> {
        if self.absent {
            Err(EnvironmentError::NotFound)
        } else {
            Err(EnvironmentError::OutcomeUnknown("not confirmed".into()))
        }
    }
}
fn request() -> CreateEnvironment {
    serde_json::from_value(json!({"tenant":"tenant","id":"adx-id","execution":{
        "image":"app:1","isolation_runtime":"runc","inherit_entrypoint":true,"entrypoint":[],"working_dir":"","user":null,"env":{"MODEL_KEY":"test-key"},"resources":{"cpu_millis":1000,"memory_mib":512},"service":[]
    }})).unwrap()
}
#[tokio::test]
async fn local_adapter_passes_the_public_request_and_preserves_unknown_delete() {
    let app = Arc::new(Application::default());
    let environment =
        LocalEnvironment::new(app.clone(), Duration::from_secs(60), network_policy()).unwrap();
    assert!(environment.create(&request()).await.unwrap().ready);
    let calls = app.creates.lock().unwrap().clone();
    assert_eq!(calls[0].0, "tenant");
    assert_eq!(
        calls[0].1,
        json!({"namespace":"adx","name":"id","image":"app:1","runtime":"runc","inheritEntrypoint":true,"env":{"MODEL_KEY":"test-key"},"cpu":1000,"memory":512,"createTimeoutSeconds":60,"ports":[],"network":adx_activator::environment_request::network_input(&network_policy()).unwrap()})
    );
    assert!(calls[0].2.starts_with("activator-create-"));
    assert!(
        environment
            .get("tenant", "adx-id")
            .await
            .unwrap()
            .unwrap()
            .ready
    );
    assert!(matches!(
        environment.delete("tenant", "adx-id").await,
        Err(EnvironmentError::OutcomeUnknown(_))
    ));
}
#[tokio::test]
async fn expired_calls_are_not_submitted_and_submitted_timeouts_remain_unknown() {
    let app = Arc::new(Application {
        stall: true,
        ..Default::default()
    });
    let environment =
        LocalEnvironment::new(app.clone(), Duration::from_secs(60), network_policy()).unwrap();
    let mut request = request();
    request.deadline_unix_ms = Some(adx_agent_core::unix_time_millis().saturating_sub(1));
    assert!(matches!(
        environment.create(&request).await,
        Err(EnvironmentError::Unavailable(_))
    ));
    assert!(app.creates.lock().unwrap().is_empty());
    request.deadline_unix_ms = Some(adx_agent_core::unix_time_millis() + 50);
    assert!(matches!(
        environment.create(&request).await,
        Err(EnvironmentError::OutcomeUnknown(_))
    ));
    assert_eq!(app.creates.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn local_adapter_preserves_explicit_delete_absence() {
    let app = Arc::new(Application {
        absent: true,
        ..Default::default()
    });
    let environment =
        LocalEnvironment::new(app, Duration::from_secs(60), network_policy()).unwrap();
    assert!(matches!(
        environment.delete("tenant", "adx-id").await,
        Err(EnvironmentError::NotFound)
    ));
}

#[tokio::test]
async fn local_adapter_observes_dormancy_without_lifecycle_side_effects() {
    for (state, phase) in [
        ("pausing", EnvironmentPhase::Pausing),
        ("paused", EnvironmentPhase::Paused),
        ("resuming", EnvironmentPhase::Resuming),
    ] {
        let app = Arc::new(Application {
            state: Some(state),
            ..Default::default()
        });
        let environment =
            LocalEnvironment::new(app.clone(), Duration::from_secs(1), network_policy()).unwrap();
        let observed = environment.get("tenant", "adx-id").await.unwrap().unwrap();
        assert_eq!(observed.phase, phase);
        assert!(!observed.ready);
        assert!(app.creates.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn local_creation_attempt_changes_only_the_operation_identity() {
    let app = Arc::new(Application::default());
    let environment =
        LocalEnvironment::new(app.clone(), Duration::from_secs(60), network_policy()).unwrap();
    let mut request = request();
    environment.create(&request).await.unwrap();
    environment.create(&request).await.unwrap();
    request.creation_attempt = 1;
    environment.create(&request).await.unwrap();
    let calls = app.creates.lock().unwrap();
    assert_eq!(calls[0], calls[1]);
    assert_ne!(calls[0].2, calls[2].2);
    assert_eq!(calls[0].1, calls[2].1);
}
