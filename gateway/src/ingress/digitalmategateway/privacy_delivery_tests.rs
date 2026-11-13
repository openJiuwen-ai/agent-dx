//! Real PostgreSQL and HTTP identity boundary, controlled Binding/provider input.
use super::super::{
    composio::WebhookVerifier,
    ingestion::{DeliveryAuthorization, Inbox, InboxLimits, SubscriptionBinding},
    privacy_center::PrivacyCenter,
    privacy_cipher::PrivacyCipher,
    privacy_http::PrivacyHttp,
};
use super::*;
use adx_agent_core::{AgentBinding, EnvironmentRef, TemplateRef};
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD;
use deadpool_postgres::{Manager, Pool};
use http_body_util::{BodyExt, Full};
use std::{collections::BTreeMap, time::Duration};
use tokio::sync::Mutex;

struct CurrentBinding(Mutex<AgentBinding>);
#[async_trait]
impl BindingReader for CurrentBinding {
    async fn binding(&self, scope: &Scope) -> Result<AgentBinding, Error> {
        let current = self.0.lock().await.clone();
        if current.scope != *scope {
            return Err(Error::Unauthorized);
        }
        Ok(current)
    }
}

#[tokio::test]
#[ignore = "requires the explicit task-owned PostgreSQL database"]
async fn persisted_batch_identity_and_read_only_bootstrap_are_current_and_purpose_bound() {
    let url = std::env::var("ADX_INGESTION_TEST_DATABASE_URL").unwrap();
    let pg: tokio_postgres::Config = url.parse().unwrap();
    assert!(pg
        .get_dbname()
        .unwrap_or("")
        .starts_with("dm_ingestion_test_"));
    let pool = Pool::builder(Manager::new(pg, tokio_postgres::NoTls))
        .max_size(6)
        .build()
        .unwrap();
    let client = pool.get().await.unwrap();
    client
        .batch_execute(include_str!("../accounts/schema.sql"))
        .await
        .unwrap();
    let user = format!("delivery-{}", uuid::Uuid::new_v4());
    client.execute("INSERT INTO adx_accounts.users(user_id,provider,developer_scope,client_id,union_id,agreement_version) VALUES($1,'test','delivery-test','test',$1,'1')", &[&user]).await.unwrap();
    drop(client);
    let accounts = AccountStore::from_url(&url, true, None).await.unwrap();
    let scope = Scope {
        tenant: "tenant-delivery".into(),
        template: "digitalmate".into(),
        template_ref: TemplateRef::Version("1".into()),
        binding_id: user.clone(),
    };
    let current = Arc::new(CurrentBinding(Mutex::new(AgentBinding {
        scope: scope.clone(),
        generation: "generation-one".into(),
        current: EnvironmentRef::new("1".into(), "instance-one".into()),
        secondary: None,
        phase: BindingPhase::Active,
        parameters: BTreeMap::from([("workspace".into(), "workspace-delivery".into())]),
        volumes: BTreeMap::new(),
        creation_attempts: BTreeMap::new(),
    })));
    let signer = Arc::new(TokenSigner::new(&[7; 32], "tenant-delivery"));
    let identity = Arc::new(PrivacyIdentity::new(
        "digitalmate".into(),
        accounts.clone(),
        current.clone(),
        signer.clone(),
    ));
    let credentials = identity
        .admit_workload(&scope, "workspace-delivery", &user)
        .await
        .unwrap();
    let cipher = PrivacyCipher::new(
        "key-one".into(),
        BTreeMap::from([("key-one".into(), [7; 32])]),
    )
    .unwrap();
    let center = Arc::new(PrivacyCenter::new(accounts, cipher, signer));
    center
        .put(
            &credentials.privacy,
            "DIGITALMATE_CONFIG",
            br#"{"TEST_USER_SETTING":"synthetic"}"#,
            crate::ingress::accounts::ExpectedRevision::New,
        )
        .await
        .unwrap();
    let inbox = Inbox::initialize(
        pool.clone(),
        InboxLimits {
            max_pending_events: 100,
            max_pending_bytes: 8_000_000,
            per_subscription_events: 50,
            debounce: Duration::ZERO,
            dedupe_retention: Duration::from_secs(7 * 86400),
        },
    )
    .await
    .unwrap();
    let mut binding = SubscriptionBinding {
        tenant: scope.tenant.clone(),
        user_id: user.clone(),
        workspace: "workspace-delivery".into(),
        template: "digitalmate".into(),
        template_ref: "1".into(),
        binding_id: user.clone(),
        binding_generation: "generation-one".into(),
        service_port: 8080,
        connection_id: user.clone(),
        connection_generation: 1,
        subscription_id: user.clone(),
        subscription_generation: 1,
        trigger_id: format!("ti_{user}"),
        trigger_slug: "GMAIL_NEW_GMAIL_MESSAGE".into(),
        account_id: "ca_test".into(),
        auth_config_id: "ac_test".into(),
        composio_user_id: "dm_test".into(),
        enabled: true,
    };
    // Controlled explicit subscription registration, never a production consent substitute.
    inbox.register(&binding).await.unwrap();
    let raw = serde_json::to_vec(&serde_json::json!({"id":user,"type":"composio.trigger.message","metadata":{"trigger_id":binding.trigger_id,"trigger_slug":binding.trigger_slug,"connected_account_id":"ca_test","auth_config_id":"ac_test","user_id":"dm_test"},"data":{"message_id":"synthetic-message"}})).unwrap();
    let timestamp = now().unwrap().to_string();
    let mut signed = format!("wh_test.{timestamp}.").into_bytes();
    signed.extend_from_slice(&raw);
    let secret = b"synthetic-webhook-secret";
    let signature =
        STANDARD.encode(hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, secret), &signed).as_ref());
    let headers = http::HeaderMap::from_iter([
        (
            http::header::HeaderName::from_static("webhook-id"),
            http::HeaderValue::from_static("wh_test"),
        ),
        (
            http::header::HeaderName::from_static("webhook-timestamp"),
            timestamp.parse().unwrap(),
        ),
        (
            http::header::HeaderName::from_static("webhook-signature"),
            format!("v1,{signature}").parse().unwrap(),
        ),
    ]);
    inbox
        .receive(
            WebhookVerifier::new(&[secret])
                .unwrap()
                .verify(&headers, &raw, now().unwrap() as u64)
                .unwrap(),
        )
        .await
        .unwrap();
    let lease = inbox.claim(Duration::from_secs(30)).await.unwrap().unwrap();
    let delivery = Arc::new(DeliveryIdentity::new(identity.clone(), inbox.clone()));
    let headers = delivery
        .headers(&lease.batch)
        .await
        .unwrap_or_else(|_| panic!("actual issuer refused current batch"));
    let token = headers[http::header::AUTHORIZATION]
        .to_str()
        .unwrap()
        .strip_prefix("Bearer ")
        .unwrap();
    let admission = delivery.verify(token).await.unwrap();
    assert_eq!(admission.json()["batch_id"], lease.batch.batch_id);
    assert_eq!(admission.json()["purpose"], "ingestion.delivery");
    let http = PrivacyHttp::new(center.clone(), identity.clone()).with_ingestion(delivery.clone());
    for (path, credential, body, status) in [
        (
            "/v1/ingestion/admit",
            token,
            b"{}".as_slice(),
            http::StatusCode::OK,
        ),
        (
            "/v1/ingestion/bootstrap",
            token,
            b"{}",
            http::StatusCode::OK,
        ),
        (
            "/v1/ingestion/admit",
            credentials.privacy.as_str(),
            b"{}",
            http::StatusCode::UNAUTHORIZED,
        ),
        (
            "/v1/ingestion/bootstrap",
            credentials.connector.as_str(),
            b"{}",
            http::StatusCode::UNAUTHORIZED,
        ),
        (
            "/v1/identity/renew",
            token,
            b"{}",
            http::StatusCode::UNAUTHORIZED,
        ),
        (
            "/v1/secret/get",
            token,
            br#"{"name":"DIGITALMATE_CONFIG"}"#,
            http::StatusCode::UNAUTHORIZED,
        ),
    ] {
        let request = http::Request::builder()
            .method("POST")
            .uri(path)
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .body(Full::new(bytes::Bytes::copy_from_slice(body)))
            .unwrap();
        let response = http.handle(request).await;
        assert_eq!(response.status(), status, "{path}");
        if path.ends_with("/bootstrap") && status == http::StatusCode::OK {
            let json: serde_json::Value =
                serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            assert_eq!(
                STANDARD.decode(json["value"].as_str().unwrap()).unwrap(),
                br#"{"TEST_USER_SETTING":"synthetic"}"#
            );
            assert!(json.get("token").is_none());
        }
    }
    binding.subscription_generation += 1;
    binding.enabled = false;
    inbox.register(&binding).await.unwrap();
    assert!(matches!(
        delivery.verify(token).await,
        Err(Error::Unauthorized)
    ));
    assert!(delivery.headers(&lease.batch).await.is_err());
    pool.close();
}
