use adx_agent_core::sandbox::*;
use adx_agent_store::{AgentState, MemoryRepository};
use adx_dispatcher::{server, transport::HttpSandbox, Config, Dispatcher};
use async_trait::async_trait;
use axum::{routing::get, Json, Router};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
const TOKEN: &str = "test-service-token-at-least-32-bytes";
struct Absent;
#[async_trait]
impl Sandbox for Absent {
    async fn create(&self, _: &CreateSandbox) -> Result<SandboxObservation, SandboxError> {
        Err(SandboxError::Unsupported("test".into()))
    }
    async fn get(&self, _: &str, _: &str) -> Result<Option<SandboxObservation>, SandboxError> {
        Ok(None)
    }
    async fn delete(&self, _: &str, _: &str) -> Result<SandboxObservation, SandboxError> {
        Err(SandboxError::Unsupported("test".into()))
    }
}
async fn serve(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{address}"), task)
}
#[tokio::test]
async fn internal_api_checks_readiness_service_identity_and_session_lookup() {
    let dispatcher = Arc::new(
        Dispatcher::new(
            AgentState::new(Arc::new(MemoryRepository::default())),
            Arc::new(Absent),
            uuid::Uuid::new_v4().to_string(),
            Config::default(),
        )
        .unwrap(),
    );
    let ready = Arc::new(AtomicBool::new(false));
    let (url, task) = serve(server::router(dispatcher, TOKEN, ready.clone()).unwrap()).await;
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .get(format!("{url}/health/ready"))
            .send()
            .await
            .unwrap()
            .status(),
        503
    );
    ready.store(true, Ordering::Release);
    assert_eq!(
        client
            .get(format!("{url}/health/ready"))
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    let body = serde_json::json!({"scope":{"tenant":"t","template":"a","version":"1","session_id":"c"},"affinity_key":null});
    for token in ["", "forged"] {
        assert_eq!(
            client
                .post(format!("{url}/internal/adx/v1/resolve"))
                .bearer_auth(token)
                .json(&body)
                .send()
                .await
                .unwrap()
                .status(),
            401
        );
    }
    let response = client
        .post(format!("{url}/internal/adx/v1/resolve"))
        .bearer_auth(TOKEN)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404);
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap()["kind"],
        "not_found"
    );
    task.abort();
}
#[tokio::test]
async fn sandbox_client_rejects_mismatched_identity_and_never_replays_or_redirects() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let router = Router::new().route(
        "/api/sandbox/v2/instances/id",
        get(move || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Json(SandboxObservation {
                    id: "id".into(),
                    tenant: "wrong-tenant".into(),
                    phase: SandboxPhase::Running,
                    ready: true,
                    runtime_id: None,
                    message: None,
                })
            }
        })
        .delete(|| async { axum::response::Redirect::temporary("/wrong-place") }),
    );
    let (url, task) = serve(router).await;
    assert!(HttpSandbox::new(&url, TOKEN.into(), Duration::from_secs(1), None, false).is_err());
    let client = HttpSandbox::new(&url, TOKEN.into(), Duration::from_secs(1), None, true).unwrap();
    assert!(matches!(
        client.get("tenant", "id").await,
        Err(SandboxError::Unavailable(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        client.delete("tenant", "id").await,
        Err(SandboxError::OutcomeUnknown(_))
    ));
    task.abort();
}

#[tokio::test]
async fn sandbox_connect_failure_is_unavailable_even_for_writes() {
    // Release a listener immediately so the connect is refused before HTTP submission.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let client = HttpSandbox::new(&url, TOKEN.into(), Duration::from_secs(1), None, true).unwrap();
    let create: CreateSandbox = serde_json::from_value(serde_json::json!({
        "id":"id", "tenant":"tenant", "execution": {
            "image":"app:1", "isolation_runtime":"runc", "entrypoint":["/app/start"],
            "working_dir":"/", "user":null, "env":{}, "resources":{"cpu_millis":1000,"memory_mib":128}, "service":[]
        }
    })).unwrap();
    let result = client.create(&create).await;
    assert!(
        matches!(result, Err(SandboxError::Unavailable(_))),
        "unexpected create result: {result:?}"
    );
    assert!(matches!(
        client.delete("tenant", "id").await,
        Err(SandboxError::Unavailable(_))
    ));
    assert!(matches!(
        client.get("tenant", "id").await,
        Err(SandboxError::Unavailable(_))
    ));
}

#[tokio::test]
async fn lost_http_reply_is_unknown_only_for_writes() {
    use tokio::io::AsyncReadExt;
    for write in [true, false] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 4096];
            assert!(socket.read(&mut bytes).await.unwrap() > 0);
            // The peer received the request, then lost the reply.
        });
        let client =
            HttpSandbox::new(&url, TOKEN.into(), Duration::from_secs(1), None, true).unwrap();
        if write {
            assert!(matches!(
                client.delete("tenant", "id").await,
                Err(SandboxError::OutcomeUnknown(_))
            ));
        } else {
            assert!(matches!(
                client.get("tenant", "id").await,
                Err(SandboxError::Unavailable(_))
            ));
        }
        peer.await.unwrap();
    }
}
