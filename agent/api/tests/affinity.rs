use adx_agent_api::{activator::ActivatorClient, request::RequestContext, Error};
use adx_agent_core::{
    activator::Target,
    discovery::{ranked_endpoints, ActivatorEndpoint},
    Environment, EnvironmentPhase, Scope,
};
use axum::{extract::State, Json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
const TOKEN: &str = "test-activator-token-at-least-32-bytes";
type Calls = Arc<Mutex<Vec<(String, bool)>>>;
fn scope() -> Scope {
    Scope {
        tenant: "t".into(),
        template: "a".into(),
        version: "1".into(),
        environment_id: "env".into(),
    }
}
fn context() -> RequestContext {
    RequestContext::new(Duration::from_secs(2))
}
async fn server(id: &str) -> (ActivatorEndpoint, tokio::task::JoinHandle<()>, Calls) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let app = axum::Router::new()
        .fallback(axum::routing::post(handler))
        .with_state((id.to_owned(), calls.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = ActivatorEndpoint {
        id: id.into(),
        url: format!("http://{}", listener.local_addr().unwrap()),
    };
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (endpoint, task, calls)
}
async fn handler(
    State((id, calls)): State<(String, Calls)>,
    uri: axum::http::Uri,
    Json(value): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let bypass = value
        .get("bypasscache")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    calls.lock().unwrap().push((uri.path().into(), bypass));
    let environment = Environment {
        scope: serde_json::from_value(value["scope"].clone()).unwrap(),
        generation: id,
        sandbox_id: "sandbox".into(),
        phase: EnvironmentPhase::Active,
    };
    if uri.path().ends_with("/delete") {
        Json(serde_json::Value::Null)
    } else if uri.path().ends_with("/get") {
        Json(serde_json::to_value(environment).unwrap())
    } else {
        Json(
            serde_json::to_value(Target {
                environment,
                service: vec![],
            })
            .unwrap(),
        )
    }
}
#[tokio::test]
async fn all_env_operations_and_bypass_use_same_instance_across_gateways() {
    let (a, ta, ca) = server("a").await;
    let (b, tb, cb) = server("b").await;
    let client = ActivatorClient::new(
        vec![a.url.clone(), b.url.clone()],
        TOKEN.into(),
        Duration::from_secs(2),
        None,
        true,
    )
    .unwrap();
    let other = ActivatorClient::new(
        vec![b.url, a.url],
        TOKEN.into(),
        Duration::from_secs(2),
        None,
        true,
    )
    .unwrap();
    let first = client.activate(&context(), &scope(), None).await.unwrap();
    for _ in 0..8 {
        assert_eq!(
            other.activate(&context(), &scope(), None).await.unwrap(),
            first
        );
    }
    assert_eq!(
        client
            .activate_with_cache(
                &context(),
                &scope(),
                Some(&first.environment.generation),
                true
            )
            .await
            .unwrap(),
        first
    );
    assert_eq!(
        client.environment(&context(), &scope()).await.unwrap(),
        first.environment
    );
    client
        .delete_environment(&context(), &scope())
        .await
        .unwrap();
    let a_calls = ca.lock().unwrap();
    let b_calls = cb.lock().unwrap();
    assert!(a_calls.is_empty() != b_calls.is_empty());
    assert_eq!(a_calls.len() + b_calls.len(), 12);
    assert_eq!(
        a_calls
            .iter()
            .chain(b_calls.iter())
            .filter(|(_, bypass)| *bypass)
            .count(),
        1
    );
    ta.abort();
    tb.abort();
}
#[tokio::test]
async fn unreachable_preferred_instance_uses_second_ranked_address() {
    let (up, task, calls) = server("up").await;
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let down = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    let endpoints = vec![
        ActivatorEndpoint {
            id: up.url.clone(),
            url: up.url.clone(),
        },
        ActivatorEndpoint {
            id: down.clone(),
            url: down.clone(),
        },
    ];
    let mut selected = scope();
    for i in 0..1000 {
        selected.environment_id = i.to_string();
        if ranked_endpoints(&selected, &endpoints)[0].url == down {
            break;
        }
    }
    assert_eq!(ranked_endpoints(&selected, &endpoints)[0].url, down);
    let client = ActivatorClient::new(
        vec![up.url, down],
        TOKEN.into(),
        Duration::from_secs(2),
        None,
        true,
    )
    .unwrap();
    assert_eq!(
        client
            .activate(&context(), &selected, None)
            .await
            .unwrap()
            .environment
            .generation,
        "up"
    );
    assert_eq!(calls.lock().unwrap().len(), 1);
    task.abort();
}

#[tokio::test]
#[ignore = "requires disposable ADX_AGENT_TEST_REDIS_URL"]
async fn discovery_updates_routes_retains_snapshot_on_error_and_accepts_empty_snapshot() {
    use adx_agent_api::discovery::DiscoveryConfig;
    use adx_agent_store::discovery::RedisRegistry;
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let namespace = format!("gateway-discovery-{}", uuid::Uuid::new_v4());
    let registry = RedisRegistry::new(&url, &namespace, Duration::from_secs(2)).unwrap();
    let (a, ta, _) = server("a").await;
    let (b, tb, _) = server("b").await;
    registry
        .renew(&a, "a", Duration::from_secs(60))
        .await
        .unwrap();
    let client = ActivatorClient::with_discovery(
        DiscoveryConfig {
            redis_url: url.clone(),
            namespace: namespace.clone(),
            refresh_seconds: 1,
        },
        TOKEN.into(),
        Duration::from_secs(2),
        None,
        true,
    )
    .unwrap();
    async fn wait_for(client: &ActivatorClient, scope: &Scope, expected: Option<&str>) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let result = client.activate(&context(), scope, None).await;
                match (&result, expected) {
                    (Ok(target), Some(id)) if target.environment.generation == id => return,
                    (Err(Error::Unavailable(_)), None) => return,
                    _ => tokio::time::sleep(Duration::from_millis(25)).await,
                }
            }
        })
        .await
        .unwrap();
    }
    let mut selected = scope();
    for i in 0..1000 {
        selected.environment_id = i.to_string();
        if ranked_endpoints(&selected, &[a.clone(), b.clone()])[0].id == "b" {
            break;
        }
    }
    wait_for(&client, &selected, Some("a")).await;
    registry
        .renew(&b, "b", Duration::from_secs(60))
        .await
        .unwrap();
    wait_for(&client, &selected, Some("b")).await;
    registry.unregister("b", "b").await.unwrap();
    wait_for(&client, &selected, Some("a")).await;
    // Corrupt only this test's lease index to make refresh fail; cached routing must survive.
    let mut admin = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let leases = format!("adx:v2:{namespace}:activators:leases");
    let _: () = redis::cmd("SET")
        .arg(&leases)
        .arg("invalid-type")
        .query_async(&mut admin)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        client
            .activate(&context(), &selected, None)
            .await
            .unwrap()
            .environment
            .generation,
        "a"
    );
    let _: u64 = redis::cmd("DEL")
        .arg(&leases)
        .query_async(&mut admin)
        .await
        .unwrap();
    wait_for(&client, &selected, None).await;
    registry.unregister("a", "a").await.unwrap();
    ta.abort();
    tb.abort();
}
