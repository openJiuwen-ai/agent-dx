//! Real PostgreSQL and HTTP identity boundary, controlled Binding/provider input.
use super::super::{
    application_ws::{Application, ApplicationWsConfig},
    composio::WebhookVerifier,
    connector_owner::{ConnectorWorkloadIdentity, OwnerAuthority, OwnerOperation},
    ingestion::{DeliveryAuthorization, Inbox, InboxLimits, SubscriptionBinding},
    privacy_center::PrivacyCenter,
    privacy_cipher::PrivacyCipher,
    privacy_http::PrivacyHttp,
};
use super::*;
use crate::ingress::accounts::{BusinessAuth, Principal};
use adx_agent_core::{AgentBinding, EnvironmentRef};
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
async fn persisted_batch_identity_is_current_and_cannot_read_user_configuration() {
    exercise_delivery(false, false, false).await;
}

#[tokio::test]
#[ignore = "requires the explicit task-owned PostgreSQL database"]
async fn https_client_uses_current_delivery_identity_and_rejects_revoked_subscription() {
    exercise_delivery(true, false, false).await;
}

#[tokio::test]
#[ignore = "requires the explicit task-owned PostgreSQL database"]
async fn delivery_bootstrap_exchanges_only_bounded_connector_identity() {
    exercise_delivery(false, true, false).await;
}

#[tokio::test]
#[ignore = "requires task-owned PostgreSQL and the paired installed mail consumer"]
async fn paired_installed_mail_consumer_accepts_actual_gateway_v2() {
    exercise_delivery(true, true, true).await;
}

struct NoSession;
#[async_trait]
impl BusinessAuth for NoSession {
    async fn login(
        &self,
        _: crate::ingress::accounts::LoginRequest,
    ) -> Result<crate::ingress::accounts::LoginResponse, Error> {
        Err(Error::Unauthorized)
    }
    async fn authenticate(&self, _: &str) -> Result<Principal, Error> {
        Err(Error::Unauthorized)
    }
    async fn check(&self, _: &Principal) -> Result<(), Error> {
        Err(Error::Unauthorized)
    }
    async fn logout(&self, _: &str) -> Result<(), Error> {
        Err(Error::Unauthorized)
    }
    async fn quota(&self, _: &Principal) -> Result<crate::ingress::accounts::QuotaResponse, Error> {
        Err(Error::Unauthorized)
    }
    async fn model_parameters(
        &self,
        _: &Principal,
    ) -> Result<adx_agent_core::mounts::Parameters, Error> {
        panic!("delivery bootstrap cannot obtain model credentials")
    }
}

async fn exercise_delivery(https_client: bool, bootstrap: bool, installed_consumer: bool) {
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
    let application = ApplicationWsConfig {
        request_timeout_seconds: 5,
        template: "digitalmate".into(),
        template_ref: "1".into(),
        backend_path: "/ws".into(),
        service_port: 8080,
    };
    let scope = application
        .scope("tenant-delivery", &user, Application::DigitalMate)
        .unwrap();
    let workspace =
        super::super::entrypoint::workspace_parameters("tenant-delivery", "digitalmate", &user)
            .unwrap()
            .remove("workspace")
            .unwrap();
    let current = Arc::new(CurrentBinding(Mutex::new(AgentBinding {
        scope: scope.clone(),
        generation: "generation-one".into(),
        current: EnvironmentRef::new("1".into(), "instance-one".into()),
        secondary: None,
        phase: BindingPhase::Active,
        parameters: BTreeMap::from([("workspace".into(), workspace.clone())]),
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
        .admit_workload(&scope, &workspace, &user)
        .await
        .unwrap();
    // This is the real random authorization record, not an incarnation alias:
    // reconnect reuses it; revocation rotates it without changing the Binding.
    let first_ref = signer
        .verify_connector(&credentials.connector, false)
        .unwrap()
        .id;
    let reconnect = identity
        .admit_workload(&scope, &workspace, &user)
        .await
        .unwrap();
    assert_eq!(
        signer
            .verify_connector(&reconnect.connector, false)
            .unwrap()
            .id,
        first_ref
    );
    let generation_before = current.0.lock().await.generation.clone();
    accounts.privacy_revoke_grant(&first_ref).await.unwrap();
    let renewed_authority = identity
        .admit_workload(&scope, &workspace, &user)
        .await
        .unwrap();
    let renewed_ref = signer
        .verify_connector(&renewed_authority.connector, false)
        .unwrap()
        .id;
    assert_ne!(renewed_ref, first_ref);
    assert_eq!(current.0.lock().await.generation, generation_before);
    assert!(identity
        .connector_identity(&credentials.connector)
        .await
        .is_err());
    let credentials = renewed_authority;
    let cipher = PrivacyCipher::new(
        "key-one".into(),
        BTreeMap::from([("key-one".into(), [7; 32])]),
    )
    .unwrap();
    let center = Arc::new(PrivacyCenter::new(accounts, cipher, signer.clone()));
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
        workspace: workspace.clone(),
        template: "digitalmate".into(),
        template_ref: "1".into(),
        binding_id: scope.binding_id.clone(),
        authority_ref: identity
            .signer
            .verify_connector(&credentials.connector, false)
            .unwrap()
            .id,
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
    let owner = Arc::new(
        OwnerAuthority::new(
            "tenant-delivery".into(),
            application,
            signer.clone(),
            current.clone(),
            Arc::new(NoSession),
        )
        .unwrap()
        .with_identity(identity.clone()),
    );
    let normal_proof = identity
        .connector_identity(&credentials.connector)
        .await
        .unwrap();
    assert_eq!(normal_proof["grant_ref"], binding.authority_ref);
    let target = owner.delivery_target(&binding).await.unwrap();
    assert_eq!(target["binding_generation"], "generation-one");
    assert!(target.get("environment_id").is_none());
    assert!(target.get("service_port").is_none());
    current.0.lock().await.current.environment_id = "replacement-execution".into();
    assert_eq!(owner.delivery_target(&binding).await.unwrap(), target);
    for field in [
        "tenant",
        "user",
        "workspace",
        "template",
        "ref",
        "id",
        "authority",
        "disabled",
    ] {
        let mut wrong = binding.clone();
        match field {
            "tenant" => wrong.tenant = "other".into(),
            "user" => wrong.user_id = "other".into(),
            "workspace" => wrong.workspace = "other".into(),
            "template" => wrong.template = "other".into(),
            "ref" => wrong.template_ref = "2".into(),
            "id" => wrong.binding_id = "other".into(),
            "authority" => wrong.authority_ref = "0".repeat(64),
            _ => wrong.enabled = false,
        }
        assert!(owner.delivery_target(&wrong).await.is_err(), "{field}");
    }
    let mut http =
        PrivacyHttp::new(center.clone(), identity.clone()).with_ingestion(delivery.clone());
    if bootstrap {
        http = http.with_authorization_owner(owner.clone());
    }
    let http = Arc::new(http);
    let grant_count: i64 = pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT count(*) FROM adx_accounts.privacy_grants WHERE user_id=$1",
            &[&user],
        )
        .await
        .unwrap()
        .get(0);
    let mut bootstrap_credential = None;
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
            if bootstrap {
                http::StatusCode::OK
            } else {
                http::StatusCode::SERVICE_UNAVAILABLE
            },
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
        if path.ends_with("/bootstrap") && status == http::StatusCode::SERVICE_UNAVAILABLE {
            let json: serde_json::Value =
                serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            assert_eq!(json, serde_json::json!({"error":"AUTH_UNAVAILABLE"}));
            assert!(json.get("value").is_none());
            assert!(json.get("token").is_none());
        } else if path.ends_with("/bootstrap") && status == http::StatusCode::OK {
            let json: serde_json::Value =
                serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            assert_eq!(json.as_object().unwrap().len(), 4);
            assert_eq!(json["purpose"], "ingestion.connector-bootstrap");
            assert_eq!(json["target"]["user_id"], user);
            assert_eq!(json["target"]["workspace"], workspace);
            assert_eq!(json["target"]["binding_id"], scope.binding_id);
            assert_eq!(json["target"]["binding_generation"], "generation-one");
            assert!(json["target"].get("environment_id").is_none());
            assert!(json["target"].get("service_port").is_none());
            assert_eq!(json["target"].as_object().unwrap().len(), 7);
            let credential = json["connector_token"].as_str().unwrap().to_owned();
            assert!(credential.starts_with("dm-connector-v1."));
            let claims = signer.verify_connector(&credential, false).unwrap();
            assert_eq!(claims.bootstrap_until, Some(claims.exp));
            assert_eq!(claims.recovery_until, claims.exp);
            assert!(claims.exp <= admission.claims.exp);
            assert_eq!(json["expires_at"], claims.exp);
            assert!(signer.verify(&credential, false).is_err());
            bootstrap_credential = Some(credential);
        }
    }
    if let Some(credential) = bootstrap_credential {
        let count: i64 = pool
            .get()
            .await
            .unwrap()
            .query_one(
                "SELECT count(*) FROM adx_accounts.privacy_grants WHERE user_id=$1",
                &[&user],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            count, grant_count,
            "bootstrap must reuse the existing grant"
        );
        let proof = identity.connector_identity(&credential).await.unwrap();
        assert_eq!(proof["renewable"], false);
        assert_eq!(proof["bootstrap_until"], proof["expires_at"]);
        assert_eq!(proof["recovery_expires_at"], proof["expires_at"]);
        assert!(proof["bootstrap_until"].as_i64().unwrap() <= admission.claims.exp);
        assert!(matches!(
            identity.renew_connector(&credential).await,
            Err(Error::Forbidden)
        ));
        let headers = http::HeaderMap::from_iter([(
            http::header::AUTHORIZATION,
            format!("Bearer {credential}").parse().unwrap(),
        )]);
        let workload = ConnectorWorkloadIdentity::authenticate(identity.clone(), &headers)
            .await
            .unwrap();
        assert!(owner
            .workload_headers(&workload, OwnerOperation::Intent, b"{}")
            .await
            .is_ok());
        for (path, body, status) in [
            (
                "/v1/connector/identity",
                b"{}".as_slice(),
                http::StatusCode::OK,
            ),
            ("/v1/connector/renew", b"{}", http::StatusCode::FORBIDDEN),
            (
                "/v1/secret/get",
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
            assert_eq!(http.handle(request).await.status(), status, "{path}");
        }
        let request = http::Request::builder()
            .method("POST")
            .uri("/v1/ingestion/bootstrap")
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Full::new(bytes::Bytes::from_static(
                br#"{"user_id":"forged"}"#,
            )))
            .unwrap();
        assert_eq!(
            http.handle(request).await.status(),
            http::StatusCode::BAD_REQUEST
        );
    }
    let https = if https_client {
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        let certificate = fixture.join("ingress-cert.pem");
        let tls = adx_transport::tls::http_server_acceptor(
            &certificate,
            fixture.join("ingress-key.pem"),
            None,
            vec![b"http/1.1".to_vec()],
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!(
            "https://localhost:{}",
            listener.local_addr().unwrap().port()
        );
        let (shutdown, receiver) = tokio::sync::watch::channel(false);
        let server = tokio::spawn(http.clone().serve(listener, tls, receiver));
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(10))
            .add_root_certificate(
                reqwest::Certificate::from_pem(&std::fs::read(&certificate).unwrap()).unwrap(),
            )
            .build()
            .unwrap();
        let reply = client
            .post(format!("{origin}/v1/ingestion/admit"))
            .bearer_auth(token)
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap();
        assert_eq!(reply.status(), http::StatusCode::OK);
        let actual: serde_json::Value = reply.json().await.unwrap();
        assert_eq!(actual, admission.json());
        if installed_consumer {
            use tokio::io::AsyncWriteExt;
            let python = std::env::var("ADX_INGESTION_CONSUMER_PYTHON")
                .expect("explicit installed consumer Python");
            let probe = std::env::var("ADX_INGESTION_CONSUMER_PROBE")
                .expect("explicit paired acceptance probe");
            let scope: serde_json::Map<String, serde_json::Value> = [
                "tenant",
                "user_id",
                "workspace",
                "binding_id",
                "template",
                "template_ref",
                "purpose",
            ]
            .into_iter()
            .map(|key| (key.to_owned(), actual[key].clone()))
            .collect();
            let mut child = tokio::process::Command::new(python)
                .arg("-B")
                .arg(probe)
                .env("DM_TEST_GATEWAY_ORIGIN", &origin)
                .env("DM_TEST_CA_FILE", &certificate)
                .env("DM_TEST_DELIVERY_TOKEN", token)
                .env("DM_TEST_SCOPE_JSON", serde_json::to_string(&scope).unwrap())
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .kill_on_drop(true)
                .spawn()
                .expect("paired consumer process");
            child
                .stdin
                .take()
                .unwrap()
                .write_all(&serde_json::to_vec(&lease.batch).unwrap())
                .await
                .unwrap();
            let result = tokio::time::timeout(Duration::from_secs(30), child.wait_with_output())
                .await
                .unwrap()
                .unwrap();
            assert!(
                result.status.success(),
                "actual installed consumer rejected Gateway contract"
            );
            let summary: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
            assert_eq!(summary["gateway_identity"], "actual_https");
            assert_eq!(summary["wire_version"], 2);
            assert_eq!(summary["accepted_members"], 1);
            assert_eq!(summary["duplicate_members"], 1);
            println!("paired_installed_consumer: {summary}");
        }

        Some((client, origin, shutdown, server))
    } else {
        None
    };
    binding.subscription_generation += 1;
    binding.enabled = false;
    inbox.register(&binding).await.unwrap();
    assert!(matches!(
        delivery.verify(token).await,
        Err(Error::Unauthorized)
    ));
    assert!(delivery.headers(&lease.batch).await.is_err());
    if bootstrap {
        assert!(delivery.bootstrap(token, &owner).await.is_err());
    }
    if let Some((client, origin, shutdown, server)) = https {
        let reply = client
            .post(format!("{origin}/v1/ingestion/admit"))
            .bearer_auth(token)
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap();
        assert_eq!(reply.status(), http::StatusCode::UNAUTHORIZED);
        shutdown.send(true).unwrap();
        server.await.unwrap().unwrap();
    }
    let mut active = binding.clone();
    active.enabled = true;
    current.0.lock().await.generation = "rebuilt-workspace".into();
    assert!(owner.delivery_target(&active).await.is_err());
    assert!(identity
        .connector_identity(&credentials.connector)
        .await
        .is_err());
    pool.close();
}
