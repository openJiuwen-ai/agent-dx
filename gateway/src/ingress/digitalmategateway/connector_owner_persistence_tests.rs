//! Real persisted grants and HTTP verification, with controlled Binding metadata.
use super::super::super::{
    connector_owner::{ConnectorWorkloadIdentity, VERIFY_PATH},
    privacy_center::PrivacyCenter,
    privacy_cipher::PrivacyCipher,
    privacy_grants::PrivacyIdentity,
    privacy_http::PrivacyHttp,
};
use super::*;
use crate::ingress::accounts::AccountStore;
use deadpool_postgres::{Manager, Pool};
use http_body_util::{BodyExt, Full};

#[tokio::test]
#[ignore = "requires the explicit task-owned PostgreSQL database"]
async fn owner_purpose_persisted_workload_needs_no_phone_and_rechecks_real_grant() {
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
        .batch_execute(include_str!("../accounts/schema.sql"))
        .await
        .unwrap();
    let user = format!("owner-{}", uuid::Uuid::new_v4());
    client.execute("INSERT INTO adx_accounts.users(user_id,provider,developer_scope,client_id,union_id,agreement_version) VALUES($1,'test','owner-test','test',$1,'1')", &[&user]).await.unwrap();
    let sessions: i64 = client
        .query_one(
            "SELECT count(*) FROM adx_accounts.sessions WHERE user_id=$1",
            &[&user],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        sessions, 0,
        "no mobile session is created for this workload"
    );
    let store = AccountStore::from_url(&url, true, None).await.unwrap();
    let application = ApplicationWsConfig {
        request_timeout_seconds: 5,
        template: "digitalmate".into(),
        template_ref: "1".into(),
        backend_path: "/ws".into(),
        service_port: 18092,
    };
    let scope = application
        .scope("tenant", &user, Application::DigitalMate)
        .unwrap();
    let workspace = workspace_parameters("tenant", "digitalmate", &user)
        .unwrap()
        .remove("workspace")
        .unwrap();
    let bindings = Arc::new(Bindings(Mutex::new(AgentBinding {
        scope: scope.clone(),
        generation: "owner-generation".into(),
        current: EnvironmentRef::new("1".into(), "owner-environment".into()),
        secondary: None,
        phase: BindingPhase::Active,
        parameters: [("workspace".into(), workspace.clone())].into(),
        volumes: Default::default(),
        creation_attempts: Default::default(),
    })));
    let signer = Arc::new(TokenSigner::new(&[7; 32], "tenant"));
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
    // This auth rejects every session for our random workload user.
    let owner = Arc::new(
        OwnerAuthority::new(
            "tenant".into(),
            application,
            signer.clone(),
            bindings.clone(),
            Arc::new(Auth(AtomicBool::new(true))),
        )
        .unwrap(),
    );
    let center = Arc::new(PrivacyCenter::new(
        store,
        PrivacyCipher::new("test".into(), [("test".into(), [7; 32])].into()).unwrap(),
        signer,
    ));
    let http = PrivacyHttp::new(center, identity.clone()).with_authorization_owner(owner.clone());
    let headers = |token: &str| {
        http::HeaderMap::from_iter([(
            http::header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        )])
    };
    let workload =
        ConnectorWorkloadIdentity::authenticate(identity.clone(), &headers(&credentials.connector))
            .await
            .unwrap();
    assert_eq!(workload.user_id(), user);
    assert_eq!(workload.workspace(), workspace);
    assert_eq!(workload.binding_scope(), &scope);
    assert!(ConnectorWorkloadIdentity::authenticate(
        identity.clone(),
        &headers(&credentials.privacy)
    )
    .await
    .is_err());
    let proof = identity
        .connector_identity(&credentials.connector)
        .await
        .unwrap();
    assert_eq!(proof["renewable"], true);
    assert!(proof.as_object().unwrap().contains_key("bootstrap_until"));
    assert!(proof["bootstrap_until"].is_null());
    assert!(proof["recovery_expires_at"].as_i64().unwrap() > proof["expires_at"].as_i64().unwrap());
    let renewed = identity
        .renew_connector(&credentials.connector)
        .await
        .unwrap();
    assert_eq!(renewed["renewable"], true);
    assert!(renewed.as_object().unwrap().contains_key("bootstrap_until"));
    assert!(renewed["bootstrap_until"].is_null());
    let renewed_token = renewed["token"].as_str().unwrap();
    let renewed_proof = identity.connector_identity(renewed_token).await.unwrap();
    for field in [
        "tenant",
        "user_id",
        "workspace",
        "template",
        "template_ref",
        "binding_id",
        "binding_generation",
    ] {
        assert_eq!(proof[field], renewed_proof[field], "{field}");
    }
    assert_eq!(renewed["expires_at"], renewed_proof["expires_at"]);
    assert_eq!(
        renewed["recovery_expires_at"],
        renewed_proof["recovery_expires_at"]
    );
    assert!(
        renewed["recovery_expires_at"].as_i64().unwrap() > renewed["expires_at"].as_i64().unwrap()
    );
    let body = br#"{"connection_id":"synthetic"}"#;
    let mut last_token = String::new();
    for operation in [
        OwnerOperation::Connection,
        OwnerOperation::Metadata,
        OwnerOperation::Intent,
        OwnerOperation::Read,
    ] {
        let packet = owner
            .workload_headers(&workload, operation, body)
            .await
            .unwrap();
        let credential = token(&packet);
        let check = serde_json::to_vec(
            &serde_json::json!({"operation":operation,"body_sha256":digest(body)}),
        )
        .unwrap();
        let request = http::Request::builder()
            .method("POST")
            .uri(VERIFY_PATH)
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .body(Full::new(bytes::Bytes::from(check)))
            .unwrap();
        let response = http.handle(request).await;
        assert_eq!(response.status(), http::StatusCode::OK);
        let proof: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(proof["actor"], "connector_workload");
        assert_eq!(proof["user_id"], user);
        assert_eq!(proof["workspace"], workspace);
        assert_eq!(proof["environment_id"], "owner-environment");
        assert_eq!(proof["operation"], serde_json::to_value(operation).unwrap());
        assert!(proof.get("permissions").is_none());
        assert!(proof.get("read_history").is_none());
        for path in [
            "/v1/secret/get",
            "/v1/connector/identity",
            "/v1/ingestion/admit",
        ] {
            let request = http::Request::builder()
                .method("POST")
                .uri(path)
                .header("authorization", format!("Bearer {credential}"))
                .header("content-type", "application/json")
                .body(Full::new(bytes::Bytes::from_static(b"{}")))
                .unwrap();
            assert_eq!(
                http.handle(request).await.status(),
                http::StatusCode::UNAUTHORIZED,
                "{path}"
            );
        }
        last_token = credential.to_owned();
    }
    for operation in [
        OwnerOperation::Current,
        OwnerOperation::Effect,
        OwnerOperation::Claim,
        OwnerOperation::Record,
        OwnerOperation::Cancel,
    ] {
        assert!(matches!(
            owner.workload_headers(&workload, operation, body).await,
            Err(accounts::Error::Forbidden)
        ));
    }
    client
        .execute(
            "UPDATE adx_accounts.users SET status='disabled' WHERE user_id=$1",
            &[&user],
        )
        .await
        .unwrap();
    assert!(workload.check().await.is_err());
    assert!(identity.connector_identity(renewed_token).await.is_err());
    assert!(identity.renew_connector(renewed_token).await.is_err());
    assert!(owner
        .verify(
            &last_token,
            OwnerOperation::Read,
            &digest(body),
            Some(&identity)
        )
        .await
        .is_err());
    client
        .execute(
            "UPDATE adx_accounts.users SET status='active' WHERE user_id=$1",
            &[&user],
        )
        .await
        .unwrap();
    assert!(workload.check().await.is_ok());
    bindings.0.lock().unwrap().generation = "new-generation".into();
    assert!(workload.check().await.is_err());
    assert!(identity.connector_identity(renewed_token).await.is_err());
    assert!(identity.renew_connector(renewed_token).await.is_err());
    assert!(owner
        .verify(
            &last_token,
            OwnerOperation::Read,
            &digest(body),
            Some(&identity)
        )
        .await
        .is_err());
    // Reverting metadata cannot resurrect the grant revoked by its real owner.
    bindings.0.lock().unwrap().generation = "owner-generation".into();
    assert!(
        ConnectorWorkloadIdentity::authenticate(identity, &headers(&credentials.connector))
            .await
            .is_err()
    );
    assert!(owner
        .workload_headers(&workload, OwnerOperation::Read, body)
        .await
        .is_err());
}
