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
fn credentials() -> std::collections::BTreeMap<String, String> {
    [("tenant".into(), TOKEN.into())].into()
}
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
    let router = Router::new()
        .route(
            "/api/instances",
            get(move || {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Json(serde_json::json!([{ "id":"wrong-id", "status":"running" }]))
                }
            }),
        )
        .route(
            "/api/sandbox/v1/sandboxes/adx-id",
            axum::routing::delete(|| async { axum::response::Redirect::temporary("/wrong-place") }),
        );
    let (url, task) = serve(router).await;
    assert!(HttpSandbox::new(&url, credentials(), Duration::from_secs(1), None, false).is_err());
    let client = HttpSandbox::new(&url, credentials(), Duration::from_secs(1), None, true).unwrap();
    assert!(matches!(
        client.get("tenant", "adx-id").await,
        Err(SandboxError::Unavailable(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        client.delete("tenant", "adx-id").await,
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
    let client = HttpSandbox::new(&url, credentials(), Duration::from_secs(1), None, true).unwrap();
    let create: CreateSandbox = serde_json::from_value(serde_json::json!({
        "id":"adx-id", "tenant":"tenant", "execution": {
            "image":"app:1", "isolation_runtime":"runc", "inherit_entrypoint":true, "entrypoint":[],
            "working_dir":"", "user":null, "env":{}, "resources":{"cpu_millis":1000,"memory_mib":128}, "service":[]
        }
    })).unwrap();
    let result = client.create(&create).await;
    assert!(
        matches!(result, Err(SandboxError::Unavailable(_))),
        "unexpected create result: {result:?}"
    );
    assert!(matches!(
        client.delete("tenant", "adx-id").await,
        Err(SandboxError::Unavailable(_))
    ));
    assert!(matches!(
        client.get("tenant", "adx-id").await,
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
            HttpSandbox::new(&url, credentials(), Duration::from_secs(1), None, true).unwrap();
        if write {
            assert!(matches!(
                client.delete("tenant", "adx-id").await,
                Err(SandboxError::OutcomeUnknown(_))
            ));
        } else {
            assert!(matches!(
                client.get("tenant", "adx-id").await,
                Err(SandboxError::Unavailable(_))
            ));
        }
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn sandbox_create_uses_the_callers_remaining_deadline() {
    let calls = Arc::new(AtomicUsize::new(0));
    let received = Arc::new(std::sync::Mutex::new(None));
    let count = calls.clone();
    let seen = received.clone();
    let (url, task) = serve(Router::new().route(
        "/api/sandbox/v1/sandboxes",
        axum::routing::post(move |Json(body): Json<serde_json::Value>| {
            let count = count.clone();
            let seen = seen.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                *seen.lock().unwrap() = Some(body);
                tokio::time::sleep(Duration::from_secs(2)).await;
                axum::http::StatusCode::SERVICE_UNAVAILABLE
            }
        }),
    ))
    .await;
    let client = HttpSandbox::new(&url, credentials(), Duration::from_secs(5), None, true).unwrap();
    let deadline = adx_agent_core::unix_time_millis() + 250;
    let body = serde_json::json!({
        "id":"adx-id", "tenant":"tenant", "deadline_unix_ms":deadline, "execution": {
            "image":"app:1", "isolation_runtime":"runc", "inherit_entrypoint":true, "entrypoint":[],
            "working_dir":"", "user":null, "env":{}, "resources":{"cpu_millis":1000,"memory_mib":128}, "service":[]
        }
    });
    let create: CreateSandbox = serde_json::from_value(body.clone()).unwrap();
    let started = std::time::Instant::now();
    assert!(matches!(
        client.create(&create).await,
        Err(SandboxError::OutcomeUnknown(_))
    ));
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        received.lock().unwrap().as_ref().unwrap()["createTimeoutSeconds"],
        5
    );
    let mut expired = body;
    expired["deadline_unix_ms"] =
        serde_json::json!(adx_agent_core::unix_time_millis().saturating_sub(1));
    let expired: CreateSandbox = serde_json::from_value(expired).unwrap();
    assert!(matches!(
        client.create(&expired).await,
        Err(SandboxError::Unavailable(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    task.abort();
}

#[tokio::test]
async fn apiserver_contract_preserves_identity_tenant_credentials_and_idempotency() {
    use axum::{
        extract::Query,
        http::HeaderMap,
        routing::{delete, post},
    };
    use base64::Engine;
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let capture = seen.clone();
    let router = Router::new()
        .route("/api/sandbox/v1/sandboxes", post(move |headers: HeaderMap, Json(body): Json<serde_json::Value>| {
            let capture = capture.clone();
            async move {
                assert_eq!(headers["authorization"], format!("Bearer {TOKEN}"));
                assert_eq!(body["namespace"], "adx");
                assert_eq!(body["name"], "id");
                assert_eq!(body["image"], "app:1");
                assert_eq!(body["runtime"], "runc");
                assert_eq!(body["cpu"], 1000);
                assert_eq!(body["memory"], 128);
                assert_eq!(body["inheritEntrypoint"], true);
                assert_eq!(body["env"]["MODEL_KEY"], "test-model-key");
                assert!(body.get("tenant").is_none());
                capture.lock().unwrap().push((headers["x-request-id"].to_str().unwrap().to_string(), body));
                Json(serde_json::json!({"code":200,"data":base64::engine::general_purpose::STANDARD.encode(
                    r#"{"sandboxId":"adx-id","instanceId":"adx-id","status":"running"}"#
                )}))
            }
        }))
        .route("/api/instances", get(|headers: HeaderMap, Query(query): Query<std::collections::HashMap<String, String>>| async move {
            assert_eq!(headers["authorization"], format!("Bearer {TOKEN}"));
            assert_eq!(query.len(), 1);
            assert_eq!(query["instance_id"], "adx-id");
            Json(serde_json::json!([{"id":"adx-id", "status":"running"}]))
        }))
        .route("/api/sandbox/v1/sandboxes/adx-id", delete(|headers: HeaderMap| async move {
            assert!(headers["x-request-id"].to_str().unwrap().starts_with("activator-delete-"));
            Json(serde_json::json!({"code":200,"data":null}))
        }));
    let (url, task) = serve(router).await;
    let client = HttpSandbox::new(&url, credentials(), Duration::from_secs(5), None, true).unwrap();
    let mut create: CreateSandbox = serde_json::from_value(serde_json::json!({
        "id":"adx-id", "tenant":"tenant", "execution": {
            "image":"app:1", "isolation_runtime":"runc", "inherit_entrypoint":true, "entrypoint":[],
            "working_dir":"", "user":null, "env":{"MODEL_KEY":"test-model-key"}, "resources":{"cpu_millis":1000,"memory_mib":128}, "service":[]
        }
    })).unwrap();
    assert_eq!(
        client.create(&create).await.unwrap().phase,
        SandboxPhase::Running
    );
    create.deadline_unix_ms = Some(adx_agent_core::unix_time_millis() + 1000);
    assert_eq!(client.create(&create).await.unwrap().id, "adx-id");
    {
        let calls = seen.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], calls[1]);
    }
    assert!(client.get("tenant", "adx-id").await.unwrap().unwrap().ready);
    assert_eq!(
        client.delete("tenant", "adx-id").await.unwrap().phase,
        SandboxPhase::Deleted
    );
    assert!(matches!(
        client.get("unconfigured", "adx-id").await,
        Err(SandboxError::NotFound)
    ));
    task.abort();
}

#[tokio::test]
async fn apiserver_absence_does_not_confirm_deletion() {
    let (url, task) =
        serve(Router::new().fallback(|| async { axum::http::StatusCode::NOT_FOUND })).await;
    let client = HttpSandbox::new(&url, credentials(), Duration::from_secs(1), None, true).unwrap();
    assert!(client.get("tenant", "adx-id").await.unwrap().is_none());
    assert!(matches!(
        client.delete("tenant", "adx-id").await,
        Err(SandboxError::OutcomeUnknown(_))
    ));
    task.abort();
}

#[tokio::test]
async fn tenant_keys_are_selected_without_forwarding_tenant_query() {
    use axum::{extract::Query, http::HeaderMap};
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let (url, task) = serve(Router::new().route(
        "/api/instances",
        get(
            move |headers: HeaderMap,
                  Query(query): Query<std::collections::BTreeMap<String, String>>| {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(query.len(), 1);
                    let id = query.get("instance_id").unwrap();
                    assert_eq!(headers["authorization"], format!("Bearer key-{id}"));
                    Json(serde_json::json!([{"id":id,"status":"running"}]))
                }
            },
        ),
    ))
    .await;
    let client = HttpSandbox::new(
        &url,
        [
            ("a".into(), "key-adx-a".into()),
            ("b".into(), "key-adx-b".into()),
        ]
        .into(),
        Duration::from_secs(1),
        None,
        true,
    )
    .unwrap();
    for tenant in ["a", "b"] {
        let value = client
            .get(tenant, &format!("adx-{tenant}"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(value.tenant, tenant);
        assert!(value.ready);
    }
    assert!(matches!(
        client.delete("unknown", "adx-id").await,
        Err(SandboxError::NotFound)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    task.abort();
}

#[tokio::test]
async fn invalid_or_incomplete_write_success_is_never_confirmed() {
    use axum::routing::delete;
    for body in [
        serde_json::json!({"code":200}),
        serde_json::json!({"code":200,"data":{}}),
        serde_json::json!({"code":503,"data":null}),
    ] {
        let (url, task) = serve(Router::new().route(
            "/api/sandbox/v1/sandboxes/adx-id",
            delete(move || {
                let body = body.clone();
                async move { Json(body) }
            }),
        ))
        .await;
        let client =
            HttpSandbox::new(&url, credentials(), Duration::from_secs(1), None, true).unwrap();
        assert!(matches!(
            client.delete("tenant", "adx-id").await,
            Err(SandboxError::OutcomeUnknown(_))
        ));
        task.abort();
    }
}

#[tokio::test]
async fn forbidden_queries_do_not_become_absence_or_leak_server_details() {
    use axum::http::StatusCode;
    let (url, task) = serve(
        Router::new().fallback(|| async { (StatusCode::FORBIDDEN, "private upstream detail") }),
    )
    .await;
    let client = HttpSandbox::new(&url, credentials(), Duration::from_secs(1), None, true).unwrap();
    assert_eq!(
        client.get("tenant", "adx-id").await.unwrap_err(),
        SandboxError::NotFound
    );
    assert_eq!(
        client.delete("tenant", "adx-id").await.unwrap_err(),
        SandboxError::NotFound
    );
    task.abort();
}
