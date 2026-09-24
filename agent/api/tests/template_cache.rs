use adx_agent_api::{
    activator::ActivatorClient, managed::ManagedService, request::RequestContext, Error,
};
use adx_agent_core::TemplateVersion;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
fn context() -> RequestContext {
    RequestContext::new(Duration::from_secs(2))
}
#[tokio::test]
async fn template_cache_coalesces_loads_isolates_tenants_and_retries_invalid_results() {
    let calls = Arc::new(AtomicUsize::new(0));
    let state = calls.clone();
    let app = axum::Router::new().route("/internal/adx/v1/templates/get", axum::routing::post(move || {
        let calls = state.clone(); async move {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            if n == 0 { return (axum::http::StatusCode::NOT_FOUND, axum::Json(serde_json::to_value(Error::NotFound).unwrap())); }
            tokio::time::sleep(Duration::from_millis(20)).await;
            (axum::http::StatusCode::OK, axum::Json(serde_json::json!({"name":if n == 1 {"wrong"} else {"app"},"version":"1","image":"app:1","isolation_runtime":"runc","entrypoint":["/start"],"resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080}]})))
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let service = ManagedService::new(Arc::new(
        ActivatorClient::new(
            vec![url],
            "test-activator-token-at-least-32-bytes".into(),
            Duration::from_secs(2),
            None,
            true,
        )
        .unwrap(),
    ));
    assert!(matches!(
        service.template(&context(), "t", "app", "1").await,
        Err(Error::NotFound)
    ));
    assert!(matches!(
        service.template(&context(), "t", "app", "1").await,
        Err(Error::Unavailable(_))
    ));
    let ctx = context();
    let (a, b) = tokio::join!(
        service.template(&ctx, "t", "app", "1"),
        service.template(&ctx, "t", "app", "1")
    );
    let template: TemplateVersion = a.unwrap();
    assert_eq!(b.unwrap(), template);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    service.template(&context(), "t", "app", "1").await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    service
        .template(&context(), "other", "app", "1")
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    assert!(matches!(
        service
            .template(&RequestContext::new(Duration::ZERO), "t", "app", "1")
            .await,
        Err(Error::Unavailable(_))
    ));
    task.abort();
}
