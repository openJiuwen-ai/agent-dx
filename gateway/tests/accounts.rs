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
    async fn exchange(&self, code: &str) -> Result<VerifiedIdentity> {
        if code == "invalid" {
            Err(Error::Unauthorized)
        } else {
            let (subject, nickname) = code
                .split_once('|')
                .map_or((code, None), |(s, n)| (s, Some(n)));
            Ok(VerifiedIdentity {
                subject: subject.into(),
                display_name: nickname.map(str::to_owned),
            })
        }
    }
}
struct AgcIdentity;
#[async_trait]
impl AgcIdentityProvider for AgcIdentity {
    async fn verify(&self, access_token: &str) -> Result<VerifiedIdentity> {
        if access_token == "invalid" {
            Err(Error::InvalidAgcAccessToken)
        } else {
            Ok(VerifiedIdentity {
                subject: access_token.into(),
                display_name: None,
            })
        }
    }
}
#[derive(Default)]
struct ModelState {
    users: Mutex<HashMap<String, Value>>,
    keys: Mutex<HashMap<String, Value>>,
    lose_response: AtomicBool,
    creates: AtomicUsize,
    updates: AtomicUsize,
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
        Some(v) => (
            StatusCode::OK,
            Json(json!({"user_id":q["user_id"],"user_info":v})),
        ),
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
    let mut info = v;
    info["spend"] = json!(0.0);
    s.users
        .lock()
        .unwrap()
        .insert(info["user_id"].as_str().unwrap().into(), info.clone());
    (StatusCode::OK, Json(info))
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
    assert_eq!(v["models"], json!(["all-proxy-models"]));
    assert_eq!(v["max_budget"], 1.0);
    let key = v["key"].as_str().unwrap().to_owned();
    assert!(key.starts_with("sk-"));
    let mut info = v;
    info["spend"] = json!(0.0);
    s.keys
        .lock()
        .unwrap()
        .insert(hex::encode(Sha256::digest(key.as_bytes())), info.clone());
    s.creates.fetch_add(1, Ordering::SeqCst);
    if s.lose_response.swap(false, Ordering::SeqCst) {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({})));
    }
    (StatusCode::OK, Json(info))
}
async fn key_update(
    State(s): State<Arc<ModelState>>,
    headers: HeaderMap,
    Json(v): Json<Value>,
) -> (StatusCode, Json<Value>) {
    assert!(authorized(&headers));
    assert!(
        v.as_object()
            .unwrap()
            .keys()
            .all(|k| matches!(k.as_str(), "key" | "models" | "key_alias")),
        "only access and alias may change"
    );
    if let Some(models) = v.get("models") {
        assert_eq!(models, &json!(["all-proxy-models"]));
    }
    let key = v["key"].as_str().unwrap();
    assert_eq!(key.len(), 64, "updates use the existing key hash");
    let mut keys = s.keys.lock().unwrap();
    let info = keys.get_mut(key).unwrap();
    for field in ["models", "key_alias"] {
        if let Some(value) = v.get(field) {
            info[field] = value.clone();
        }
    }
    s.updates.fetch_add(1, Ordering::SeqCst);
    if s.lose_response.swap(false, Ordering::SeqCst) {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({})));
    }
    (StatusCode::OK, Json(json!({})))
}

async fn serve_model(state: Arc<ModelState>) -> (String, tokio::task::JoinHandle<()>) {
    let app = Router::new()
        .route("/user/info", get(user_info))
        .route("/user/new", post(user_new))
        .route("/key/info", get(key_info))
        .route("/key/generate", post(key_new))
        .route("/key/update", post(key_update))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, server)
}

#[tokio::test]
async fn litellm_expands_existing_access_without_rotating_key_or_resetting_budget() {
    let model = Arc::new(ModelState::default());
    let key = "sk-existing-user";
    let hash = hex::encode(Sha256::digest(key.as_bytes()));
    let original = json!({"user_id":"owner","models":["old-model"],"max_budget":5.0,
        "spend":1.25,"budget_duration":"30d","rpm_limit":30,"tpm_limit":30000});
    model
        .users
        .lock()
        .unwrap()
        .insert("owner".into(), json!({"user_id":"owner"}));
    model
        .keys
        .lock()
        .unwrap()
        .insert(hash.clone(), original.clone());
    let (url, server) = serve_model(model.clone()).await;
    let provider = LiteLlm::with_key(config(&url).litellm, "mock-admin".into()).unwrap();
    model.lose_response.store(true, Ordering::SeqCst);
    assert!(provider.ensure("owner", key, "huawei-owner").await.is_err());
    provider.ensure("owner", key, "huawei-owner").await.unwrap();
    provider.ensure("owner", key, "huawei-owner").await.unwrap();
    let mut expected = original;
    expected["models"] = json!(["all-proxy-models"]);
    expected["key_alias"] = json!("huawei-owner");
    assert_eq!(model.keys.lock().unwrap()[&hash], expected);
    assert_eq!(model.updates.load(Ordering::SeqCst), 1);
    assert_eq!(model.creates.load(Ordering::SeqCst), 0);
    assert_eq!(
        provider.quota("owner", key).await.unwrap().quota.balance,
        3.75
    );
    assert!(matches!(
        provider.ensure("other-owner", key, "huawei-owner").await,
        Err(Error::Forbidden)
    ));
    model.keys.lock().unwrap().get_mut(&hash).unwrap()["blocked"] = json!(true);
    assert!(matches!(
        provider.ensure("owner", key, "huawei-owner").await,
        Err(Error::Forbidden)
    ));
    assert_eq!(model.updates.load(Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test]
async fn litellm_updates_nickname_alias_without_changing_credentials_or_budget() {
    let model = Arc::new(ModelState::default());
    let key = "sk-nickname-owner";
    let hash = hex::encode(Sha256::digest(key.as_bytes()));
    let original = json!({"user_id":"owner","models":["all-proxy-models"],
        "key_alias":"old-alias","max_budget":5.0,"spend":1.5,
        "budget_duration":"30d","budget_reset_at":"2026-11-01T00:00:00Z",
        "rpm_limit":30,"tpm_limit":30000,"metadata":{"keep":"value"}});
    model
        .keys
        .lock()
        .unwrap()
        .insert(hash.clone(), original.clone());
    let (url, server) = serve_model(model.clone()).await;
    let provider = LiteLlm::with_key(config(&url).litellm, "mock-admin".into()).unwrap();
    provider
        .ensure("owner", key, "huawei-小明-12345678")
        .await
        .unwrap();
    let mut expected = original;
    expected["key_alias"] = json!("huawei-小明-12345678");
    assert_eq!(model.keys.lock().unwrap()[&hash], expected);
    assert_eq!(model.updates.load(Ordering::SeqCst), 1);
    assert_eq!(model.creates.load(Ordering::SeqCst), 0);
    provider
        .ensure("owner", key, "huawei-新昵称-12345678")
        .await
        .unwrap();
    expected["key_alias"] = json!("huawei-新昵称-12345678");
    assert_eq!(model.keys.lock().unwrap()[&hash], expected);
    server.abort();
}

fn config(url: &str) -> AccountConfig {
    serde_json::from_value(json!({"developer_scope":"test-developer","agreement_version":"1","database_url_env":"UNUSED","database_allow_plaintext":true,"litellm":{"management_url":url,"api_base":format!("{url}/v1"),"admin_key_env":"UNUSED","max_budget":1.0,"budget_duration":null,"rpm_limit":10,"tpm_limit":1000,"allow_plaintext":true}})).unwrap()
}
fn login(code: &str) -> LoginRequest {
    LoginRequest {
        authorization_code: code.into(),
        agreement_version: Some("1".into()),
    }
}

#[tokio::test]
async fn litellm_quota_uses_key_budget_and_spend_one_to_one() {
    let model = Arc::new(ModelState::default());
    let (url, server) = serve_model(model.clone()).await;
    let provider = LiteLlm::with_key(config(&url).litellm, "mock-admin".into()).unwrap();
    let key = "sk-test-user-key";
    provider.ensure("owner", key, "huawei-owner").await.unwrap();
    let digest = hex::encode(Sha256::digest(key.as_bytes()));
    {
        let mut keys = model.keys.lock().unwrap();
        keys.get_mut(&digest).unwrap()["spend"] = json!(0.25);
    }
    let quota = provider.quota("owner", key).await.unwrap();
    assert_eq!(
        (quota.quota.total, quota.quota.used, quota.quota.balance),
        (1.0, 0.25, 0.75)
    );
    assert!(!quota.quota.exhausted);
    assert!(quota.quota.reset_period.is_empty());
    {
        let mut keys = model.keys.lock().unwrap();
        keys.get_mut(&digest).unwrap()["spend"] = json!(1.1);
    }
    let quota = provider.quota("owner", key).await.unwrap();
    assert!(quota.quota.exhausted);
    assert!(quota.quota.balance < 0.0);
    server.abort();
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
    let (url, server) = serve_model(model.clone()).await;
    let provider_scope = format!("test-{}", uuid::Uuid::new_v4());
    let make = || async {
        let mut cfg = config(&url);
        cfg.developer_scope = provider_scope.clone();
        cfg.agc = Some(AgcConfig {
            project_id: format!("{provider_scope}-agc"),
            api_client_id: "test-api-client".into(),
            api_client_secret_env: "UNUSED".into(),
        });
        let model = Arc::new(LiteLlm::with_key(cfg.litellm.clone(), "mock-admin".into()).unwrap());
        Arc::new(
            AccountService::new_with_agc(
                cfg,
                "test-app".into(),
                AccountStore::from_url(&db, true, None).await.unwrap(),
                Arc::new(Identity),
                Some(Arc::new(AgcIdentity)),
                model,
                CredentialCipher::new([8; 32], "tenant").unwrap(),
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
    let (first, second) = tokio::join!(a.login(login("alice|小明")), b.login(login("alice|小明")));
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first.user_id, second.user_id);
    assert_ne!(first.token, second.token);
    let agc_request = || AgcLoginRequest {
        agc_access_token: "alice".into(),
        agreement_version: Some("1".into()),
    };
    let (agc_first, agc_second) =
        tokio::join!(a.agc_login(agc_request()), b.agc_login(agc_request()));
    let agc_first = agc_first.unwrap();
    let agc_second = agc_second.unwrap();
    assert_eq!(agc_first.user_id, agc_second.user_id);
    assert_ne!(agc_first.user_id, first.user_id, "providers are not linked");
    let pa = a.authenticate(&first.token).await.unwrap();
    let pb = b.authenticate(&second.token).await.unwrap();
    let bob = b.login(login("bob")).await.unwrap();
    let bob = b.authenticate(&bob.token).await.unwrap();
    assert_ne!(bob.user_id, pa.user_id);
    assert!(
        a.model_parameters(&pa).await.is_err(),
        "provider response was lost after key creation"
    );
    let alice_key = b.model_parameters(&pb).await.unwrap();
    assert_eq!(model.creates.load(Ordering::SeqCst), 1);
    assert_eq!(b.quota(&pb).await.unwrap().quota.balance, 1.0);
    let (left, right) = tokio::join!(a.model_parameters(&pa), b.model_parameters(&pb));
    assert_eq!(left.unwrap(), right.unwrap());
    assert_eq!(model.creates.load(Ordering::SeqCst), 1);
    let alice_hash = hex::encode(Sha256::digest(alice_key["model_api_key"].as_bytes()));
    let short_id = &pa.user_id[..8];
    assert_eq!(
        model.keys.lock().unwrap()[&alice_hash]["key_alias"],
        format!("huawei-小明-{short_id}")
    );
    let relogin = b.login(login("alice|新昵称")).await.unwrap();
    assert_eq!(relogin.user_id, pa.user_id);
    // Already-issued sessions read the latest stored nickname when reconciling.
    assert_eq!(a.model_parameters(&pa).await.unwrap(), alice_key);
    assert_eq!(
        model.keys.lock().unwrap()[&alice_hash]["key_alias"],
        format!("huawei-新昵称-{short_id}")
    );
    b.login(login("alice")).await.unwrap();
    a.model_parameters(&pa).await.unwrap();
    assert_eq!(
        model.keys.lock().unwrap()[&alice_hash]["key_alias"],
        format!("huawei-新昵称-{short_id}")
    );
    let bob_key = a.model_parameters(&bob).await.unwrap();
    assert_ne!(alice_key["model_api_key"], bob_key["model_api_key"]);
    let row = client
        .query_one(
            "SELECT encrypted_key FROM adx_accounts.model_credentials WHERE user_id=$1",
            &[&pa.user_id],
        )
        .await
        .unwrap();
    let encrypted: Vec<u8> = row.get(0);
    assert!(!encrypted
        .windows(8)
        .any(|v| v == &alice_key["model_api_key"].as_bytes()[..8]));
    let user_ids = vec![
        pa.user_id.clone(),
        bob.user_id.clone(),
        agc_first.user_id.clone(),
    ];
    assert_eq!(
        client
            .query_one(
                "SELECT count(*) FROM adx_accounts.users WHERE user_id=ANY($1)",
                &[&user_ids]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        3
    );
    a.logout(&first.token).await.unwrap();
    assert!(matches!(
        b.authenticate(&first.token).await,
        Err(Error::Unauthorized)
    ));
    b.check(&pb).await.unwrap();
    assert_eq!(b.model_parameters(&pb).await.unwrap(), alice_key);
    client
        .execute(
            "UPDATE adx_accounts.sessions SET expires_at=0 WHERE user_id=$1",
            &[&pa.user_id],
        )
        .await
        .unwrap();
    assert!(matches!(a.check(&pb).await, Err(Error::Unauthorized)));
    client
        .execute(
            "UPDATE adx_accounts.users SET status='disabled' WHERE user_id=$1",
            &[&bob.user_id],
        )
        .await
        .unwrap();
    assert!(matches!(a.check(&bob).await, Err(Error::AccountDisabled)));
    assert!(matches!(
        a.login(login("bob")).await,
        Err(Error::AccountDisabled)
    ));
    for table in ["model_credentials", "sessions", "users"] {
        client
            .execute(
                &format!("DELETE FROM adx_accounts.{table} WHERE user_id=ANY($1)"),
                &[&user_ids],
            )
            .await
            .unwrap();
    }
    server.abort();
    db_task.abort();
}
