#![cfg(feature = "agent-api")]
use async_trait::async_trait;
use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use data_plane_gateway::ingress::accounts::*;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

struct Identity;
#[async_trait]
impl IdentityProvider for Identity {
    async fn exchange(&self, code: &str) -> Result<String> {
        if code == "invalid" {
            Err(Error::Unauthorized)
        } else {
            Ok(code.into())
        }
    }
}
#[derive(Default)]
struct ModelState {
    users: Mutex<HashMap<String, Value>>,
    keys: Mutex<HashMap<String, Value>>,
    lose_response: AtomicBool,
    creates: AtomicUsize,
}
fn authorized(headers: &HeaderMap) -> bool {
    headers
        .get("authorization")
        .is_some_and(|v| v == "Bearer mock-admin")
}
async fn user_info(
    State(s): State<Arc<ModelState>>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> (StatusCode, Json<Value>) {
    if !authorized(&headers) {
        return (StatusCode::UNAUTHORIZED, Json(json!({})));
    }
    match s.users.lock().unwrap().get(&q["user_id"]).cloned() {
        Some(v) => (StatusCode::OK, Json(json!({"user_info":v}))),
        None => (StatusCode::NOT_FOUND, Json(json!({}))),
    }
}
async fn user_new(
    State(s): State<Arc<ModelState>>,
    headers: HeaderMap,
    Json(v): Json<Value>,
) -> (StatusCode, Json<Value>) {
    if !authorized(&headers) {
        return (StatusCode::UNAUTHORIZED, Json(json!({})));
    }
    assert_eq!(v["auto_create_key"], false);
    assert_eq!(v["user_role"], "internal_user");
    s.users
        .lock()
        .unwrap()
        .insert(v["user_id"].as_str().unwrap().into(), v.clone());
    (StatusCode::OK, Json(v))
}
async fn key_info(
    State(s): State<Arc<ModelState>>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> (StatusCode, Json<Value>) {
    if !authorized(&headers) {
        return (StatusCode::UNAUTHORIZED, Json(json!({})));
    }
    assert_eq!(q["key"].len(), 64, "management query must use a hash");
    match s.keys.lock().unwrap().get(&q["key"]).cloned() {
        Some(v) => (StatusCode::OK, Json(json!({"info":v}))),
        None => (StatusCode::NOT_FOUND, Json(json!({}))),
    }
}
async fn key_new(
    State(s): State<Arc<ModelState>>,
    headers: HeaderMap,
    Json(v): Json<Value>,
) -> (StatusCode, Json<Value>) {
    if !authorized(&headers) {
        return (StatusCode::UNAUTHORIZED, Json(json!({})));
    }
    assert_eq!(v["key_type"], "llm_api");
    assert_eq!(v["models"], json!(["test-model"]));
    assert_eq!(v["max_budget"], 1.0);
    let key = v["key"].as_str().unwrap();
    assert!(key.starts_with("sk-"));
    s.keys
        .lock()
        .unwrap()
        .insert(hex::encode(Sha256::digest(key.as_bytes())), v.clone());
    s.creates.fetch_add(1, Ordering::SeqCst);
    if s.lose_response.swap(false, Ordering::SeqCst) {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({})));
    }
    (StatusCode::OK, Json(v))
}
fn config(tenant: &str, url: &str) -> AccountConfig {
    serde_json::from_value(json!({"tenant":tenant,"developer_scope":"test-developer","agreement_version":"1","database_url_env":"UNUSED","database_allow_plaintext":true,"litellm":{"management_url":url,"api_base":format!("{url}/v1"),"admin_key_env":"UNUSED","model":"test-model","max_budget":1.0,"budget_duration":"30d","rpm_limit":10,"tpm_limit":1000,"allow_plaintext":true}})).unwrap()
}
fn login(code: &str) -> LoginRequest {
    LoginRequest {
        authorization_code: code.into(),
        agreement_version: Some("1".into()),
    }
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL with accounts/schema.sql applied"]
async fn postgres_sessions_and_litellm_credentials_survive_retries_and_replicas() {
    let db =
        std::env::var("ADX_ACCOUNT_TEST_DATABASE_URL").expect("dedicated test database required");
    let (client, connection) = tokio_postgres::connect(&db, tokio_postgres::NoTls)
        .await
        .unwrap();
    let db_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let model = Arc::new(ModelState::default());
    model.lose_response.store(true, Ordering::SeqCst);
    let app = Router::new()
        .route("/user/info", get(user_info))
        .route("/user/new", post(user_new))
        .route("/key/info", get(key_info))
        .route("/key/generate", post(key_new))
        .with_state(model.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let tenant = format!("test-{}", uuid::Uuid::new_v4());
    let make = || async {
        let cfg = config(&tenant, &url);
        let model = Arc::new(LiteLlm::with_key(cfg.litellm.clone(), "mock-admin".into()).unwrap());
        Arc::new(
            AccountService::new(
                cfg,
                "test-app".into(),
                AccountStore::from_url(&db, true, None).await.unwrap(),
                Arc::new(Identity),
                model,
                [8; 32],
            )
            .unwrap(),
        )
    };
    let a = make().await;
    let b = make().await;
    assert!(matches!(
        a.login(LoginRequest {
            authorization_code: "alice".into(),
            agreement_version: None
        })
        .await,
        Err(Error::AgreementRequired)
    ));
    assert!(matches!(
        a.login(login("invalid")).await,
        Err(Error::Unauthorized)
    ));
    let (first, second) = tokio::join!(a.login(login("alice")), b.login(login("alice")));
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first.user_id, second.user_id);
    assert_ne!(first.token, second.token);
    let pa = a.authenticate(&first.token).await.unwrap();
    let pb = b.authenticate(&second.token).await.unwrap();
    let bob = b.login(login("bob")).await.unwrap();
    let bob = b.authenticate(&bob.token).await.unwrap();
    assert_ne!(bob.user_id, pa.user_id);
    assert!(
        a.launch_config(&pa).await.is_err(),
        "provider response was lost after key creation"
    );
    let alice_key = b.launch_config(&pb).await.unwrap();
    assert_eq!(model.creates.load(Ordering::SeqCst), 1);
    let (left, right) = tokio::join!(a.launch_config(&pa), b.launch_config(&pb));
    assert_eq!(left.unwrap(), right.unwrap());
    assert_eq!(model.creates.load(Ordering::SeqCst), 1);
    let bob_key = a.launch_config(&bob).await.unwrap();
    assert_ne!(alice_key.env["API_KEY"], bob_key.env["API_KEY"]);
    let row=client.query_one("SELECT encrypted_key FROM adx_accounts.model_credentials WHERE tenant=$1 AND user_id=$2",&[&tenant,&pa.user_id]).await.unwrap();
    let encrypted: Vec<u8> = row.get(0);
    assert!(!encrypted
        .windows(8)
        .any(|v| v == &alice_key.env["API_KEY"].as_bytes()[..8]));
    assert_eq!(
        client
            .query_one(
                "SELECT count(*) FROM adx_accounts.users WHERE tenant=$1",
                &[&tenant]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    a.logout(&first.token).await.unwrap();
    assert!(matches!(
        b.authenticate(&first.token).await,
        Err(Error::Unauthorized)
    ));
    b.check(&pb).await.unwrap();
    assert_eq!(b.launch_config(&pb).await.unwrap(), alice_key);
    client
        .execute(
            "UPDATE adx_accounts.sessions SET expires_at=0 WHERE tenant=$1 AND user_id=$2",
            &[&tenant, &pa.user_id],
        )
        .await
        .unwrap();
    assert!(matches!(a.check(&pb).await, Err(Error::Unauthorized)));
    client
        .execute(
            "UPDATE adx_accounts.users SET status='disabled' WHERE tenant=$1 AND user_id=$2",
            &[&tenant, &bob.user_id],
        )
        .await
        .unwrap();
    assert!(matches!(a.check(&bob).await, Err(Error::Forbidden)));
    assert!(matches!(a.login(login("bob")).await, Err(Error::Forbidden)));
    for table in ["model_credentials", "sessions", "users"] {
        client
            .execute(
                &format!("DELETE FROM adx_accounts.{table} WHERE tenant=$1"),
                &[&tenant],
            )
            .await
            .unwrap();
    }
    server.abort();
    db_task.abort();
}
