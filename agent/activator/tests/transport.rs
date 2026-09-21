use adx_activator::transport::HttpSandbox;
use adx_agent_core::sandbox::*;
use axum::{routing::get, Json, Router};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
const TOKEN: &str = "test-service-token-at-least-32-bytes";
async fn serve(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{address}"), task)
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
