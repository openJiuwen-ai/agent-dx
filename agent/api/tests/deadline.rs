use adx_agent_api::{request::RequestContext, Error};
use std::time::Duration;

#[tokio::test(start_paused = true)]
async fn one_deadline_covers_read_then_write_without_restarting_the_budget() {
    let ctx = RequestContext::new(Duration::from_secs(10));
    let started = tokio::time::Instant::now();
    let result = ctx
        .run(async {
            tokio::time::sleep(Duration::from_secs(7)).await;
            ctx.start_write();
            tokio::time::sleep(Duration::from_secs(7)).await;
            Ok(())
        })
        .await;
    assert!(matches!(result, Err(Error::OutcomeUnknown(_))));
    assert_eq!(started.elapsed(), Duration::from_secs(10));
}

#[tokio::test(start_paused = true)]
async fn read_timeout_and_expired_retry_do_not_submit_an_operation() {
    let ctx = RequestContext::new(Duration::from_secs(3));
    assert!(matches!(
        ctx.run(std::future::pending::<Result<(), Error>>()).await,
        Err(Error::Unavailable(_))
    ));
    let submitted = std::sync::atomic::AtomicBool::new(false);
    assert!(matches!(
        ctx.run(async {
            submitted.store(true, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        })
        .await,
        Err(Error::Unavailable(_))
    ));
    assert!(!submitted.load(std::sync::atomic::Ordering::Relaxed));
}

#[tokio::test(start_paused = true)]
async fn a_definite_local_error_before_deadline_is_preserved() {
    let ctx = RequestContext::new(Duration::from_secs(5));
    let result: Result<(), Error> = ctx
        .run(async {
            ctx.start_write();
            tokio::time::sleep(Duration::from_secs(4)).await;
            Err(Error::Conflict("rejected".into()))
        })
        .await;
    assert!(matches!(result, Err(Error::Conflict(_))));
}

#[tokio::test]
async fn remote_calls_propagate_the_remaining_request_budget() {
    use adx_agent_api::activator::ActivatorClient;
    use axum::{http::HeaderMap, routing::post, Json, Router};
    use std::sync::{Arc, Mutex};
    let deadlines = Arc::new(Mutex::new(Vec::<u64>::new()));
    let seen = deadlines.clone();
    let app = Router::new().route("/internal/adx/v1/templates/get", post(move |headers: HeaderMap| {
        let seen = seen.clone();
        async move {
            seen.lock().unwrap().push(headers[adx_agent_core::activator::DEADLINE_HEADER].to_str().unwrap().parse().unwrap());
            tokio::time::sleep(Duration::from_millis(50)).await;
            Json(serde_json::json!({"name":"app","version":"1","image":"app:1","isolation_runtime":"runc","entrypoint":["/start"],"resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080}]}))
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let client = ActivatorClient::new(
        vec![url],
        "test-service-token-at-least-32-bytes".into(),
        Duration::from_secs(3),
        None,
        true,
    )
    .unwrap();
    let ctx = RequestContext::new(Duration::from_secs(2));
    ctx.run(async {
        client.template(&ctx, "tenant", "app", "1").await?;
        client.template(&ctx, "tenant", "app", "1").await?;
        Ok(())
    })
    .await
    .unwrap();
    let deadlines = deadlines.lock().unwrap();
    assert_eq!(deadlines.len(), 2);
    assert!(
        deadlines[0].abs_diff(deadlines[1]) < 20,
        "calls must retain the original deadline: {deadlines:?}"
    );
    server.abort();
}
