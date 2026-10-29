mod support;

use adx_activator::transport::HttpEnvironment;
use adx_agent_core::environment::*;
use axum::{http::StatusCode, routing::post, Json, Router};
use serde_json::json;
use std::{collections::BTreeMap, time::Duration};

#[tokio::test]
async fn confirmed_queue_cancellation_survives_http_transport() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route(
                "/api/environment/v1/environments",
                post(|| async {
                    (
                        StatusCode::GATEWAY_TIMEOUT,
                        Json(json!({"error": {
                            "code":"DEADLINE_EXCEEDED", "retry":"never", "outcome":"not_started",
                            "schedulingFailure": {
                                "snapshotRevision":1, "nodeCount":1, "eligibleNodes":0,
                                "requested":{"cpu_millis":0,"memory_bytes":0,"disk_bytes":0},
                                "maxCapacity":{"cpu_millis":0,"memory_bytes":0,"disk_bytes":0},
                                "maxAvailable":{"cpu_millis":0,"memory_bytes":0,"disk_bytes":0},
                                "rejections":{"waiting_for_volume":1}
                            }
                        }})),
                    )
                }),
            ),
        )
        .await
        .unwrap();
    });
    let client = HttpEnvironment::new(
        &url,
        BTreeMap::from([("tenant".into(), "token".into())]),
        Duration::from_secs(1),
        None,
        true,
        support::network_policy(),
    )
    .unwrap();
    let request =
        serde_json::from_value(json!({"id":"adx-candidate","tenant":"tenant","execution":{
            "image":"image","isolation_runtime":"runsc","inherit_entrypoint":true,
            "entrypoint":[],"working_dir":"","user":null,"env":{},
            "resources":{"cpu_millis":100,"memory_mib":128},"service":[]
        }}))
        .unwrap();
    let result = client.create(&request).await;
    assert!(
        format!("{result:?}").contains("SchedulingCancelled"),
        "confirmed cancellation must stay distinct from unknown outcomes: {result:?}"
    );
    task.abort();
}

#[tokio::test]
async fn plain_timeout_or_terminal_failure_never_authorizes_a_new_attempt() {
    for error in [
        json!({"code":"DEADLINE_EXCEEDED","retry":"same_operation","outcome":"unknown"}),
        json!({"code":"DEADLINE_EXCEEDED","retry":"never","outcome":"terminal"}),
        json!({"code":"DEADLINE_EXCEEDED","retry":"never","outcome":"not_started","schedulingFailure":{}}),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let body = json!({"error":error});
        let task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/api/environment/v1/environments",
                    post(move || {
                        let body = body.clone();
                        async move { (StatusCode::GATEWAY_TIMEOUT, Json(body)) }
                    }),
                ),
            )
            .await
            .unwrap();
        });
        let client = HttpEnvironment::new(
            &url,
            BTreeMap::from([("tenant".into(), "token".into())]),
            Duration::from_secs(1),
            None,
            true,
            support::network_policy(),
        )
        .unwrap();
        let request=serde_json::from_value(json!({"id":"adx-candidate","tenant":"tenant","execution":{
            "image":"image","isolation_runtime":"runsc","inherit_entrypoint":true,"entrypoint":[],
            "working_dir":"","user":null,"env":{},"resources":{"cpu_millis":100,"memory_mib":128},"service":[]
        }})).unwrap();
        assert!(matches!(
            client.create(&request).await,
            Err(EnvironmentError::OutcomeUnknown(_))
        ));
        task.abort();
    }
}

#[tokio::test]
async fn creation_attempt_changes_only_the_http_request_identity() {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use std::sync::{Arc, Mutex};
    let calls = Arc::new(Mutex::new(Vec::new()));
    let seen = calls.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener,Router::new().route("/api/environment/v1/environments",post(move |headers:axum::http::HeaderMap,Json(body):Json<serde_json::Value>| {
            let seen=seen.clone();async move {
                seen.lock().unwrap().push((headers["x-request-id"].to_str().unwrap().to_owned(),body));
                Json(json!({"code":200,"data":STANDARD.encode(br#"{"environmentId":"adx-candidate","status":"running"}"#)}))
            }
        }))).await.unwrap();
    });
    let client = HttpEnvironment::new(
        &url,
        BTreeMap::from([("tenant".into(), "token".into())]),
        Duration::from_secs(1),
        None,
        true,
        support::network_policy(),
    )
    .unwrap();
    let mut request:CreateEnvironment=serde_json::from_value(json!({"id":"adx-candidate","tenant":"tenant","execution":{
        "image":"image","isolation_runtime":"runsc","inherit_entrypoint":true,"entrypoint":[],
        "working_dir":"","user":null,"env":{},"resources":{"cpu_millis":100,"memory_mib":128},"service":[]
    }})).unwrap();
    client.create(&request).await.unwrap();
    client.create(&request).await.unwrap();
    request.creation_attempt = 1;
    client.create(&request).await.unwrap();
    let calls = calls.lock().unwrap();
    assert_eq!(
        calls[0], calls[1],
        "unknown retry must keep request identity and specification"
    );
    assert_ne!(calls[0].0, calls[2].0);
    assert_eq!(
        calls[0].1, calls[2].1,
        "attempt metadata must not change execution specification"
    );
    task.abort();
}
