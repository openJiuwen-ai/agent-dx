//! AC-1/AC-2: admission readiness reflects initialization (a latch, never a
//! runtime dependency probe) and draining rejects new work explicitly so
//! Gateway callers retry another member.
use adx_activator::server::{self, SharedActivator};
use adx_activator::Activator;
use adx_agent_core::environment::*;
use adx_agent_core::error::Error;
use adx_agent_store::{AgentState, MemoryRepository};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, OnceLock,
    },
    time::Duration,
};

const TOKEN: &str = "test-activator-token-at-least-32-bytes";
const SCOPE: &str =
    r#"{"scope":{"tenant":"t","template":"a","template_ref":{"version":"1"},"binding_id":"b"}}"#;

/// Every Platform call fails: the tests only exercise admission control, and
/// a failing dependency must not change readiness once initialized.
struct Unavailable;
#[async_trait::async_trait]
impl Environment for Unavailable {
    async fn create(
        &self,
        _request: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        Err(EnvironmentError::Unavailable("unavailable".into()))
    }
    async fn get(
        &self,
        _tenant: &str,
        _id: &str,
    ) -> Result<Option<EnvironmentObservation>, EnvironmentError> {
        Err(EnvironmentError::Unavailable("unavailable".into()))
    }
    async fn delete(
        &self,
        _tenant: &str,
        _id: &str,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        Err(EnvironmentError::Unavailable("unavailable".into()))
    }
}

fn activator() -> Arc<Activator> {
    Arc::new(Activator::new(
        AgentState::new(Arc::new(MemoryRepository::default())),
        Arc::new(Unavailable),
    ))
}

async fn spawn(slot: SharedActivator, draining: Arc<AtomicBool>) -> String {
    let app = server::router_with_readiness(slot, draining, TOKEN, Duration::from_secs(3)).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    url
}

async fn status(client: &reqwest::Client, url: &str, path: &str) -> reqwest::StatusCode {
    client
        .get(format!("{url}{path}"))
        .send()
        .await
        .unwrap()
        .status()
}

async fn post(client: &reqwest::Client, url: &str, path: &str) -> reqwest::Response {
    client
        .post(format!("{url}{path}"))
        .bearer_auth(TOKEN)
        .header("content-type", "application/json")
        .body(SCOPE)
        .send()
        .await
        .unwrap()
}

/// AC-1: the readiness probe reports 503 until initialization completes and
/// 204 after; requests before initialization fail with an explicit
/// unavailable error; a runtime dependency failure after initialization is a
/// normal per-request error that never flips readiness back (unlike the
/// previous PING-based probe, which un-readied the replica).
#[tokio::test]
async fn readiness_latches_at_initialization_and_survives_dependency_failures() {
    let slot: SharedActivator = Arc::new(OnceLock::new());
    let draining = Arc::new(AtomicBool::new(false));
    let url = spawn(slot.clone(), draining).await;
    let client = reqwest::Client::new();

    assert_eq!(
        status(&client, &url, "/health/ready").await,
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "uninitialized replica is not ready"
    );
    assert_eq!(
        status(&client, &url, "/health/live").await,
        reqwest::StatusCode::NO_CONTENT,
        "liveness has no dependencies"
    );
    let response = post(&client, &url, "/internal/adx/v1/bindings/get").await;
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert!(matches!(
        response.json::<Error>().await.unwrap(),
        Error::Unavailable(_)
    ));

    // Initialization completes. A read is served normally (missing binding
    // → 404) and an activation hits the failing Platform dependency
    // (→ 503 unavailable); readiness stays latched through both. The template
    // is published first: an unknown template fails with 404 before Platform
    // is ever reached, which would not exercise the dependency path.
    let activator = activator();
    let _ = slot.set(activator.clone());
    let draft: adx_agent_core::TemplateDraft = serde_json::from_value(serde_json::json!({
        "name": "a",
        "image": "test-image",
        "isolation_runtime": "runc",
        "resources": {"cpu_millis": 100, "memory_mib": 64}
    }))
    .unwrap();
    activator.publish("t", &draft).await.unwrap();
    assert_eq!(
        status(&client, &url, "/health/ready").await,
        reqwest::StatusCode::NO_CONTENT
    );
    assert_eq!(
        post(&client, &url, "/internal/adx/v1/bindings/get")
            .await
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    let response = post(&client, &url, "/internal/adx/v1/bindings/activate").await;
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    // Unavailability is retried within the request deadline; a persistently
    // failing Platform reports an unknown outcome for the same AgentBinding
    // retry, not a definitive failure.
    assert!(matches!(
        response.json::<Error>().await.unwrap(),
        Error::OutcomeUnknown(_)
    ));
    assert_eq!(
        status(&client, &url, "/health/ready").await,
        reqwest::StatusCode::NO_CONTENT,
        "runtime dependency failure does not flip readiness"
    );
}

/// AC-2: once draining, new work is rejected with an explicit retryable
/// error and the readiness probe turns 503; liveness stays 204.
#[tokio::test]
async fn draining_rejects_new_work_and_unsets_readiness() {
    let slot: SharedActivator = Arc::new(OnceLock::new());
    let draining = Arc::new(AtomicBool::new(false));
    let _ = slot.set(activator());
    let url = spawn(slot, draining.clone()).await;
    let client = reqwest::Client::new();
    assert_eq!(
        status(&client, &url, "/health/ready").await,
        reqwest::StatusCode::NO_CONTENT
    );

    draining.store(true, Ordering::Release);

    assert_eq!(
        status(&client, &url, "/health/ready").await,
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        status(&client, &url, "/health/live").await,
        reqwest::StatusCode::NO_CONTENT
    );
    let response = post(&client, &url, "/internal/adx/v1/bindings/get").await;
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let error = response.json::<Error>().await.unwrap();
    assert!(matches!(error, Error::Unavailable(message) if message.contains("draining")));
}

/// Backward-compatible router(): a directly-constructed Activator is treated
/// as initialized and not draining.
#[tokio::test]
async fn direct_router_stays_ready() {
    let app = server::router(activator(), TOKEN, Duration::from_secs(3)).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = reqwest::Client::new();
    assert_eq!(
        status(&client, &url, "/health/ready").await,
        reqwest::StatusCode::NO_CONTENT
    );
}
