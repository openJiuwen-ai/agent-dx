//! Separate OS processes and real Redis, with a fake Gateway Sandbox boundary.
//! This does not claim Platform/RRT acceptance.
use adx_agent_core::{sandbox::*, Scope};
use adx_agent_store::{AgentState, RedisRepository};
use adx_dispatcher::{ResolveRequest, Target};
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use std::{
    collections::HashMap,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;
const TOKEN: &str = "process-test-service-token-at-least-32-bytes";
type Pool = Arc<Mutex<HashMap<String, SandboxObservation>>>;
fn auth(headers: &HeaderMap) -> Result<(), StatusCode> {
    if headers.get("authorization").and_then(|v| v.to_str().ok())
        == Some(&format!("Bearer {TOKEN}"))
    {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}
async fn create(
    State(pool): State<Pool>,
    headers: HeaderMap,
    Json(body): Json<CreateSandbox>,
) -> Result<Json<SandboxObservation>, StatusCode> {
    auth(&headers)?;
    let key = adx_agent_core::encode_key(&[&body.tenant, &body.id]);
    Ok(Json(
        pool.lock()
            .await
            .entry(key)
            .or_insert(SandboxObservation {
                id: body.id,
                tenant: body.tenant,
                phase: SandboxPhase::Running,
                ready: true,
                runtime_id: Some("fake-1".into()),
                message: None,
            })
            .clone(),
    ))
}
async fn inspect(
    State(pool): State<Pool>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<SandboxObservation>, StatusCode> {
    auth(&headers)?;
    let tenant = query.get("tenant").ok_or(StatusCode::BAD_REQUEST)?;
    pool.lock()
        .await
        .get(&adx_agent_core::encode_key(&[tenant, &id]))
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}
async fn delete(
    State(pool): State<Pool>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<SandboxObservation>, StatusCode> {
    auth(&headers)?;
    let tenant = query.get("tenant").ok_or(StatusCode::BAD_REQUEST)?;
    let observed = SandboxObservation {
        id: id.clone(),
        tenant: tenant.clone(),
        phase: SandboxPhase::Deleted,
        ready: false,
        runtime_id: None,
        message: None,
    };
    pool.lock()
        .await
        .insert(adx_agent_core::encode_key(&[tenant, &id]), observed.clone());
    Ok(Json(observed))
}
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
async fn start(
    url: &str,
    ns: &str,
    gateway: &str,
    dir: &std::path::Path,
    name: &str,
) -> (Process, String) {
    // Release the ephemeral listener immediately before child bind; retry is a test failure if the port was stolen.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let origin = format!("http://{address}");
    let config = dir.join(format!("{name}.json"));
    std::fs::write(&config,serde_json::to_vec(&serde_json::json!({
        "listen":address.to_string(),"node_id":name,"advertised_url":origin,"redis_url":url,"namespace":ns,
        "gateway_url":gateway,"allow_plaintext_transport":true,"gateway_ca_file":null
    })).unwrap()).unwrap();
    let log = std::fs::File::create(dir.join(format!("{name}.log"))).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_adx-dispatcher"))
        .env("ADX_DISPATCHER_CONFIG", config)
        .env("ADX_DISPATCHER_SERVICE_TOKEN", TOKEN)
        .env("ADX_SANDBOX_SERVICE_TOKEN", TOKEN)
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .spawn()
        .unwrap();
    let mut process = Process(child);
    let client = reqwest::Client::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert!(
                process.0.try_wait().unwrap().is_none(),
                "child exited; see {}",
                dir.display()
            );
            if client
                .get(format!("{origin}/health/ready"))
                .send()
                .await
                .is_ok_and(|r| r.status() == 204)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    (process, origin)
}
async fn resolve(client: &reqwest::Client, origin: &str, request: &ResolveRequest) -> Target {
    let response = client
        .post(format!("{origin}/internal/adx/v1/resolve"))
        .bearer_auth(TOKEN)
        .json(request)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    response.json().await.unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable ADX_AGENT_TEST_REDIS_URL and spawns two Dispatcher processes"]
async fn two_processes_restart_without_memory_transfer() {
    let redis = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let ns = format!("process-{}", uuid::Uuid::new_v4());
    let store = Arc::new(
        RedisRepository::connect(&redis, &ns, Duration::from_secs(3))
            .await
            .unwrap(),
    );
    let state = AgentState::new(store.clone());
    state.publish("tenant",&serde_json::from_value(serde_json::json!({"name":"test","version":"1","image":"preinstalled:1","isolation_runtime":"runc","entrypoint":["/app/start"],"resources":{"cpu_millis":1000,"memory_mib":512}})).unwrap()).await.unwrap();
    let scope = Scope {
        tenant: "tenant".into(),
        template: "test".into(),
        version: "1".into(),
        session_id: "session".into(),
    };
    state.create_session(scope.clone()).await.unwrap();
    let pool = Pool::default();
    let router = Router::new()
        .route("/api/sandbox/v2/instances", post(create))
        .route("/api/sandbox/v2/instances/:id", get(inspect).delete(delete))
        .with_state(pool.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gateway = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let dir = std::env::current_dir()
        .unwrap()
        .join("../../out/agent-v2-p3/process")
        .join(&ns);
    std::fs::create_dir_all(&dir).unwrap();
    let (a, url_a) = start(&redis, &ns, &gateway, &dir, "a").await;
    let (b, url_b) = start(&redis, &ns, &gateway, &dir, "b").await;
    assert_eq!(store.dispatchers().await.unwrap().len(), 2);
    let client = reqwest::Client::new();
    let request = ResolveRequest {
        bypass_cache: false,
        scope: scope.clone(),
        affinity_key: Some("sticky".into()),
    };
    let (first, second) = tokio::join!(
        resolve(&client, &url_a, &request),
        resolve(&client, &url_b, &request)
    );
    assert_eq!(first, second);
    assert_eq!(pool.lock().await.len(), 1);
    drop(a); // Hard process exit: no cache handoff, no clean unregister.
    assert_eq!(first, resolve(&client, &url_b, &request).await);
    let (c, url_c) = start(&redis, &ns, &gateway, &dir, "replacement").await;
    assert_eq!(first, resolve(&client, &url_c, &request).await);
    let response = client
        .post(format!("{url_c}/internal/adx/v1/release"))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({"scope":scope}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 202);
    assert!(pool
        .lock()
        .await
        .values()
        .all(|i| i.phase == SandboxPhase::Deleted));
    assert!(state.session(&scope).await.unwrap().is_none());
    drop(b);
    drop(c);
    server.abort();
    eprintln!("process logs: {}", dir.display());
}
