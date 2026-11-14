//! Actual PG grants and existing TLS service with an in-repository protocol client.
//! Binding observations are controlled; no phone, provider, mailbox or report participates.
use super::super::{
    privacy_center::{BindingReader, PrivacyCenter},
    privacy_cipher::PrivacyCipher,
    privacy_grants::{now, Claims, PrivacyIdentity, TokenSigner},
    privacy_http::PrivacyHttp,
};
use crate::ingress::accounts::{AccountStore, Error};
use adx_agent_core::{AgentBinding, BindingPhase, EnvironmentRef, Scope};
use async_trait::async_trait;
use deadpool_postgres::{Manager, Pool};
use serde_json::json;
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

struct Bindings(Mutex<AgentBinding>);
#[async_trait]
impl BindingReader for Bindings {
    async fn binding(&self, _: &Scope) -> Result<AgentBinding, Error> {
        Ok(self.0.lock().unwrap().clone())
    }
}

#[tokio::test]
#[ignore = "requires the explicit task-owned PostgreSQL database"]
async fn normal_connector_renews_over_real_https_after_access_expiry_without_mobile() {
    let url = std::env::var("ADX_INGESTION_TEST_DATABASE_URL").unwrap();
    let pg: tokio_postgres::Config = url.parse().unwrap();
    assert!(pg
        .get_dbname()
        .unwrap_or("")
        .starts_with("dm_ingestion_test_"));
    let pool = Pool::builder(Manager::new(pg, tokio_postgres::NoTls))
        .max_size(4)
        .build()
        .unwrap();
    let client = pool.get().await.unwrap();
    client
        .batch_execute(include_str!("../../accounts/schema.sql"))
        .await
        .unwrap();
    let user = format!("consumer-{}", uuid::Uuid::new_v4());
    client.execute("INSERT INTO adx_accounts.users(user_id,provider,developer_scope,client_id,union_id,agreement_version) VALUES($1,'test','consumer-test','test',$1,'1')", &[&user]).await.unwrap();
    let scope = Scope {
        tenant: "tenant".into(),
        template: "digitalmate".into(),
        template_ref: "1".parse().unwrap(),
        binding_id: format!("binding-{user}"),
    };
    let workspace = format!("workspace-{user}");
    let bindings = Arc::new(Bindings(Mutex::new(AgentBinding {
        scope: scope.clone(),
        generation: "consumer-generation".into(),
        current: EnvironmentRef::new("1".into(), "consumer-environment".into()),
        secondary: None,
        phase: BindingPhase::Active,
        parameters: [("workspace".into(), workspace.clone())].into(),
        volumes: Default::default(),
        creation_attempts: Default::default(),
    })));
    let store = AccountStore::from_url(&url, true, None).await.unwrap();
    let signer = Arc::new(TokenSigner::new(&[9; 32], "tenant"));
    let identity = Arc::new(PrivacyIdentity::new(
        "digitalmate".into(),
        store.clone(),
        bindings.clone(),
        signer.clone(),
    ));
    let credentials = identity
        .admit_workload(&scope, &workspace, &user)
        .await
        .unwrap();
    // Issuer-side construction of an actually expired access token for the same
    // persisted normal grant. The consumer never decodes or signs this payload.
    let claims = signer
        .verify_connector(&credentials.connector, false)
        .unwrap();
    let mut expired = serde_json::to_value(claims).unwrap();
    expired["exp"] = json!(now().unwrap() - 1);
    let expired: Claims = serde_json::from_value(expired).unwrap();
    let token = signer.sign_connector(&expired).unwrap();
    assert!(identity.connector_identity(&token).await.is_err());
    let center = Arc::new(PrivacyCenter::new(
        store,
        PrivacyCipher::new("test".into(), [("test".into(), [9; 32])].into()).unwrap(),
        signer,
    ));
    let http = Arc::new(PrivacyHttp::new(center, identity));
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let tls = adx_transport::tls::http_server_acceptor(
        fixture.join("ingress-cert.pem"),
        fixture.join("ingress-key.pem"),
        None,
        vec![b"http/1.1".to_vec()],
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown, stop) = tokio::sync::watch::channel(false);
    let serving = tokio::spawn(http.serve(listener, tls, stop));
    let http_client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .add_root_certificate(
            reqwest::Certificate::from_pem(
                &std::fs::read(fixture.join("ingress-cert.pem")).unwrap(),
            )
            .unwrap(),
        )
        .build()
        .unwrap();
    let origin = format!("https://{address}");
    let expired = http_client
        .post(format!("{origin}/v1/connector/identity"))
        .bearer_auth(token.as_str())
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(expired.status(), http::StatusCode::UNAUTHORIZED);
    let renewed = http_client
        .post(format!("{origin}/v1/connector/renew"))
        .bearer_auth(token.as_str())
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(renewed.status(), http::StatusCode::OK);
    let renewed: serde_json::Value = renewed.json().await.unwrap();
    assert_eq!(renewed["renewable"], true);
    assert_eq!(renewed["bootstrap_until"], serde_json::Value::Null);
    assert!(
        renewed["recovery_expires_at"].as_i64().unwrap() > renewed["expires_at"].as_i64().unwrap()
    );
    let current_token = renewed["token"].as_str().unwrap();
    let current = http_client
        .post(format!("{origin}/v1/connector/identity"))
        .bearer_auth(current_token)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(current.status(), http::StatusCode::OK);
    let current: serde_json::Value = current.json().await.unwrap();
    assert_eq!(current["purpose"], "connector.tool");
    assert_eq!(current["user_id"], user);
    assert_eq!(current["workspace"], workspace);
    assert_eq!(current["binding_generation"], "consumer-generation");
    // Renewal does not create a phone session or a second grant.
    let counts = client.query_one("SELECT (SELECT count(*) FROM adx_accounts.sessions WHERE user_id=$1), (SELECT count(*) FROM adx_accounts.privacy_grants WHERE user_id=$1)", &[&user]).await.unwrap();
    assert_eq!(counts.get::<_, i64>(0), 0);
    assert_eq!(counts.get::<_, i64>(1), 1);
    bindings.0.lock().unwrap().generation = "replaced-generation".into();
    for path in ["/v1/connector/identity", "/v1/connector/renew"] {
        let replaced = http_client
            .post(format!("{origin}{path}"))
            .bearer_auth(current_token)
            .json(&json!({}))
            .send()
            .await
            .unwrap();
        assert_eq!(replaced.status(), http::StatusCode::UNAUTHORIZED);
    }
    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), serving)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
