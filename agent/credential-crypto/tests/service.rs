use adx_agent_core::{environment::*, service_grants::*, Scope, TemplateVersion};
use adx_agent_store::{AgentState, MemoryRepository};
use adx_credential_crypto::{
    access::{AccessClient, AccessConfig},
    service::{router, ServiceState},
    storage::write_private,
    Context, Keyring,
};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use http_body_util::BodyExt;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tower::ServiceExt;

const ISSUER: &str = "private-test-issuer-at-least-32-bytes";
const VERIFIER: &str = "private-test-verifier-at-least-32-bytes";
struct NoPlatform;
#[async_trait::async_trait]
impl Environment for NoPlatform {
    async fn create(
        &self,
        _: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        Err(EnvironmentError::Unsupported("not used for grants".into()))
    }
    async fn get(
        &self,
        _: &str,
        _: &str,
    ) -> Result<Option<EnvironmentObservation>, EnvironmentError> {
        Err(EnvironmentError::Unsupported("not used for grants".into()))
    }
    async fn delete(&self, _: &str, _: &str) -> Result<EnvironmentObservation, EnvironmentError> {
        Err(EnvironmentError::Unsupported("not used for grants".into()))
    }
}
fn context(workspace: &str) -> Context {
    Context {
        tenant: "digitalmate".into(),
        workspace: workspace.into(),
        purpose: "orbit-config".into(),
    }
}
fn request(path: &str, body: serde_json::Value, auth: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json");
    if let Some(token) = auth {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder.body(Body::from(body.to_string())).unwrap()
}

#[tokio::test]
async fn crypto_uses_existing_binding_authority_and_verification_identity_cannot_issue_grants() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let template:TemplateVersion=serde_json::from_value(serde_json::json!({"name":"orbit","version":"1","image":"registry/orbit:1","isolation_runtime":"runsc","resources":{"cpu_millis":1000,"memory_mib":512}})).unwrap();
    state
        .publish(
            "digitalmate",
            &adx_agent_core::TemplateDraft::from(&template),
        )
        .await
        .unwrap();
    let scope = Scope {
        tenant: "digitalmate".into(),
        template: "orbit".into(),
        template_ref: adx_agent_core::TemplateRef::Version("1".into()),
        binding_id: "alice".into(),
    };
    let binding = state
        .create_binding_with_parameters(
            scope.clone(),
            &BTreeMap::from([("workspace".into(), "alice".into())]),
        )
        .await
        .unwrap();
    let activator = Arc::new(adx_activator::Activator::new(
        state.clone(),
        Arc::new(NoPlatform),
    ));
    let control = adx_activator::server::router(activator.clone(), ISSUER, Duration::from_secs(3))
        .unwrap()
        .merge(
            adx_activator::service_grants::router(
                activator,
                ISSUER,
                VERIFIER,
                Duration::from_secs(3),
            )
            .unwrap(),
        );
    let issue = serde_json::to_value(IssueServiceGrant {
        scope: scope.clone(),
        expected_generation: binding.generation.clone(),
        purpose: ORBIT_CONFIG_PURPOSE.into(),
        ttl_seconds: 60,
    })
    .unwrap();
    assert_eq!(
        control
            .clone()
            .oneshot(request(
                "/internal/adx/v1/service-grants/issue",
                issue.clone(),
                Some(VERIFIER)
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let response = control
        .clone()
        .oneshot(request(
            "/internal/adx/v1/service-grants/issue",
            issue,
            Some(ISSUER),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let issued: IssuedServiceGrant =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(issued.context.workspace, "alice");
    // The existing Gateway control identity can verify; verify-only identity still cannot issue.
    let checked = control
        .clone()
        .oneshot(request(
            "/internal/adx/v1/service-grants/verify",
            serde_json::json!({"credential":issued.credential, "expected":issued.context}),
            Some(ISSUER),
        ))
        .await
        .unwrap();
    assert_eq!(checked.status(), StatusCode::OK);
    let checked: serde_json::Value =
        serde_json::from_slice(&checked.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(checked["allowed"], true);

    assert_eq!(
        control
            .clone()
            .oneshot(request(
                "/internal/adx/v1/bindings/delete",
                serde_json::json!({"scope":scope}),
                Some(VERIFIER)
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, control).await.unwrap();
    });
    let directory = tempfile::tempdir().unwrap();
    let verification_file = directory.path().join("verification");
    write_private(&verification_file, VERIFIER.as_bytes(), false).unwrap();
    let authorizer = AccessClient::new(&AccessConfig {
        url,
        token_file: verification_file,
        ca_file: None,
        allow_loopback_http: true,
        timeout_seconds: 2,
    })
    .unwrap();
    let app = router(ServiceState {
        keys: Keyring::new("v1".into(), BTreeMap::from([("v1".into(), [7; 32])])).unwrap(),
        authorizer,
    });
    let seal = serde_json::json!({"context":context("alice"),"plaintext":STANDARD.encode(b"private-token")});
    assert_eq!(
        app.clone()
            .oneshot(request("/v1/seal", seal.clone(), None))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let response = app
        .clone()
        .oneshot(request("/v1/seal", seal.clone(), Some(&issued.credential)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let open = serde_json::json!({"context":context("alice"),"envelope":body["envelope"]});
    let response = app
        .clone()
        .oneshot(request("/v1/open", open.clone(), Some(&issued.credential)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let plaintext: serde_json::Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(plaintext["plaintext"], STANDARD.encode(b"private-token"));
    assert_eq!(
        app.clone()
            .oneshot(request(
                "/v1/open",
                serde_json::json!({"context":context("bob"),"envelope":body["envelope"]}),
                Some(&issued.credential)
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        app.clone()
            .oneshot(request("/v1/seal", seal.clone(), Some(ISSUER)))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    let deleting = state.begin_delete(&scope).await.unwrap();
    assert_eq!(
        app.clone()
            .oneshot(request("/v1/open", open, Some(&issued.credential)))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    state.finish_delete(&deleting).await.unwrap();
    state
        .create_binding_with_parameters(
            scope,
            &BTreeMap::from([("workspace".into(), "alice".into())]),
        )
        .await
        .unwrap();
    assert_eq!(
        app.clone()
            .oneshot(request("/v1/seal", seal.clone(), Some(&issued.credential)))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    server.abort();
    // Aborting a listener does not terminate existing keep-alive connections.
    // Removing the verifier identity deterministically makes this dependency unavailable.
    std::fs::remove_file(directory.path().join("verification")).unwrap();
    assert_eq!(
        app.oneshot(request("/v1/seal", seal, Some(&issued.credential)))
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[test]
fn persisted_key_rotation_keeps_old_envelopes_readable_and_rejects_duplicate_key_ids() {
    use adx_credential_crypto::storage::KeyFile;
    let dir = tempfile::tempdir().unwrap();
    let keyring = dir.path().join("keys.json");
    KeyFile::initialize(&keyring, "v1".into()).unwrap();
    let original = KeyFile::load(&keyring)
        .unwrap()
        .seal(&context("alice"), b"secret")
        .unwrap();
    KeyFile::rotate(&keyring, "v2".into()).unwrap();
    let updated = KeyFile::load(&keyring).unwrap();
    assert_eq!(
        updated
            .open(&context("alice"), &original)
            .unwrap()
            .as_slice(),
        b"secret"
    );
    assert_eq!(
        updated.seal(&context("alice"), b"secret").unwrap().key_id,
        "v2"
    );
    let before = std::fs::read(&keyring).unwrap();
    assert!(KeyFile::rotate(&keyring, "v2".into()).is_err());
    assert_eq!(std::fs::read(&keyring).unwrap(), before);
}
