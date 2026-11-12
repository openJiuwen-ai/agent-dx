mod support;
use adx_activator::transport::HttpEnvironment;
use adx_agent_core::environment::*;
use axum::{
    http::{HeaderMap, StatusCode},
    routing::post,
    Json, Router,
};
use serde_json::{json, Value};
use std::{collections::BTreeMap, time::Duration};

#[tokio::test]
async fn terminal_conflict_requires_the_existing_message_and_matching_creation_identity() {
    for scenario in [
        "terminal",
        "generic",
        "wrong_env",
        "wrong_request",
        "unknown",
        "unsupported_terminal_outcome",
        "missing_detail",
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let handler = move |headers: HeaderMap, Json(input): Json<Value>| async move {
            assert_eq!(
                input["restartPolicy"],
                json!({
                    "maxAttempts": 3,
                    "initialBackoffSeconds": 5,
                    "maxBackoffSeconds": 60
                })
            );
            let mut body = json!({
                "code": 409,
                "message": "environment create has a terminal result; query its failure",
                "error": {
                    "code": "CONFLICT",
                    "retry": "never",
                    "outcome": "not_started",
                    "environmentId": "adx-original",
                    "requestId": headers["x-request-id"].to_str().unwrap()
                }
            });
            match scenario {
                "generic" => body["message"] = json!("environment specification conflict"),
                "wrong_env" => body["error"]["environmentId"] = json!("adx-other"),
                "wrong_request" => body["error"]["requestId"] = json!("other-create"),
                "unknown" => body["error"]["outcome"] = json!("unknown"),
                "unsupported_terminal_outcome" => body["error"]["outcome"] = json!("terminal"),
                "missing_detail" => body["error"] = Value::Null,
                _ => (),
            }
            (StatusCode::CONFLICT, Json(body))
        };
        let app = Router::new().route("/api/environment/v1/environments", post(handler));
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let environment = HttpEnvironment::new(
            &base,
            BTreeMap::from([("tenant".into(), "key".into())]),
            Duration::from_secs(1),
            None,
            true,
            support::network_policy(),
        )
        .unwrap();
        let request = serde_json::from_value(json!({
            "tenant": "tenant",
            "id": "adx-original",
            "execution": {
                "image": "image",
                "isolation_runtime": "runc",
                "inherit_entrypoint": true,
                "entrypoint": [],
                "working_dir": "",
                "user": null,
                "env": {},
                "resources": {"cpu_millis": 1000, "memory_mib": 512},
                "service": []
            }
        }))
        .unwrap();
        let error = environment.create(&request).await.unwrap_err();
        let EnvironmentError::Conflict(reason) = error else {
            panic!("unexpected error: {error:?}")
        };
        assert_eq!(
            reason == "environment create has a terminal result; query its failure",
            scenario == "terminal",
            "{scenario}: {reason}"
        );
        server.abort();
    }
}
