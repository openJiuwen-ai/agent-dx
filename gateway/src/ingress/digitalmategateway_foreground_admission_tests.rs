//! Actual account/Binding facade composition; account sessions and Environment are controlled fixtures.
use super::*;
use crate::ingress::accounts;
use http::{header, HeaderMap};
use std::sync::atomic::{AtomicBool, Ordering};

async fn setup() -> (DigitalMateGatewayApi, Arc<TestAuth>, AgentState) {
    let (_, backend) = fixture().await;
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let draft = serde_json::from_value(json!({
        "name":"digitalmate", "image":"app:1", "isolation_runtime":"runc",
        "resources":{"cpu_millis":100,"memory_mib":128},
        "service":[{"protocol":"http","port":18092}]
    }))
    .unwrap();
    state.publish("tenant", &draft).await.unwrap();
    state
        .put_alias(
            &serde_json::from_value(json!({
                "tenant":"tenant", "template":"digitalmate", "alias":"production", "seed":"stable",
                "versions":[{"version":"1", "weight":1}]
            }))
            .unwrap(),
            None,
        )
        .await
        .unwrap();
    let managed = Arc::new(ManagedService::new(Arc::new(LocalControl::new(Arc::new(
        adx_activator::Activator::new(state.clone(), backend),
    )))));
    let config = serde_json::from_value(json!({
        "tenant":"tenant", "jiuwen":{"agent_type":"assistant", "template":"demo", "version":"1"},
        "orbit":{"template":"orbit", "template_ref":"1", "backend_path":"/ws", "service_port":18092},
        "digitalmate":{"template":"digitalmate", "template_ref":"production", "backend_path":"/ws", "service_port":18092}
    })).unwrap();
    let auth = Arc::new(TestAuth(AtomicBool::new(false)));
    (
        DigitalMateGatewayApi::new(config, managed, auth.clone()).unwrap(),
        auth,
        state,
    )
}

fn headers(user: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        format!("Bearer {user}-session").parse().unwrap(),
    );
    headers
}

#[tokio::test]
async fn foreground_admission_uses_digitalmate_identity_without_model_or_activation() {
    let (api, _, state) = setup().await;
    let admission = api.foreground_admission().unwrap();
    let mut request = headers("alice");
    request.insert("x-user-id", "bob".parse().unwrap());
    request.insert("x-adx-binding-id", "untrusted".parse().unwrap());
    request.insert("x-adx-parameters", "workspace=forged".parse().unwrap());
    let identity = admission.authenticate(&request).await.unwrap();
    assert_eq!((identity.tenant(), identity.user_id()), ("tenant", "alice"));
    assert_eq!(identity.binding_scope().template, "digitalmate");
    assert_eq!(identity.binding_scope().template_ref.name(), "production");
    assert!(identity
        .binding_scope()
        .binding_id
        .starts_with("digitalmate-"));
    assert_eq!(
        identity.workspace(),
        "w-82823bcf213bef337a7f02b2e95204846a48ef0017599c509a1c5a73a745a321"
    );
    assert!(state
        .binding(identity.binding_scope())
        .await
        .unwrap()
        .is_none());
    identity.check().await.unwrap();
    // TestAuth.model_parameters always refuses: successful admission proves it
    // never asks for a model key or model/Host configuration.
    let other = admission.authenticate(&headers("bob")).await.unwrap();
    assert_ne!(identity.workspace(), other.workspace());
    assert_ne!(identity.binding_scope(), other.binding_scope());
}

#[tokio::test]
async fn foreground_admission_reuses_account_bearer_and_rechecks_revocation() {
    let (api, auth, _) = setup().await;
    let admission = api.foreground_admission().unwrap();
    assert!(matches!(
        admission.authenticate(&HeaderMap::new()).await,
        Err(accounts::Error::Unauthorized)
    ));
    let mut duplicate = headers("alice");
    duplicate.append(header::AUTHORIZATION, "Bearer bob-session".parse().unwrap());
    assert!(matches!(
        admission.authenticate(&duplicate).await,
        Err(accounts::Error::Unauthorized)
    ));
    let mut ambiguous = headers("alice");
    ambiguous.insert("x-auth", "another-credential".parse().unwrap());
    assert!(matches!(
        admission.authenticate(&ambiguous).await,
        Err(accounts::Error::Unauthorized)
    ));
    let identity = admission.authenticate(&headers("alice")).await.unwrap();
    auth.0.store(true, Ordering::SeqCst);
    assert!(matches!(
        identity.check().await,
        Err(accounts::Error::Unauthorized)
    ));
    assert!(matches!(
        admission.authenticate(&headers("alice")).await,
        Err(accounts::Error::Unauthorized)
    ));
}

#[tokio::test]
async fn foreground_admission_checks_actual_binding_workspace_and_deletion() {
    let (api, _, state) = setup().await;
    let admission = api.foreground_admission().unwrap();
    let identity = admission.authenticate(&headers("alice")).await.unwrap();
    state
        .create_binding_with_parameters(
            identity.binding_scope().clone(),
            &adx_agent_core::mounts::Parameters::from([(
                "workspace".into(),
                identity.workspace().into(),
            )]),
        )
        .await
        .unwrap();
    assert!(admission.authenticate(&headers("alice")).await.is_ok());
    state.begin_delete(identity.binding_scope()).await.unwrap();
    assert!(matches!(
        admission.authenticate(&headers("alice")).await,
        Err(accounts::Error::Forbidden)
    ));
    let bob = admission.authenticate(&headers("bob")).await.unwrap();
    state
        .create_binding_with_parameters(
            bob.binding_scope().clone(),
            &adx_agent_core::mounts::Parameters::from([(
                "workspace".into(),
                "another-workspace".into(),
            )]),
        )
        .await
        .unwrap();
    assert!(matches!(
        admission.authenticate(&headers("bob")).await,
        Err(accounts::Error::Forbidden)
    ));
}

#[tokio::test]
async fn foreground_admission_does_not_fall_back_to_jiuwen_or_orbit() {
    let (_, backend) = fixture().await;
    let managed = Arc::new(ManagedService::new(Arc::new(LocalControl::new(Arc::new(
        adx_activator::Activator::new(
            AgentState::new(Arc::new(MemoryRepository::default())),
            backend,
        ),
    )))));
    let api = DigitalMateGatewayApi::new(
        settings(),
        managed,
        Arc::new(TestAuth(AtomicBool::new(false))),
    )
    .unwrap();
    assert!(matches!(
        api.foreground_admission(),
        Err(accounts::Error::Configuration)
    ));
}

#[tokio::test]
async fn foreground_composio_mount_uses_actual_session_scope_and_denies_body_identity() {
    use crate::ingress::digitalmategateway::composio::{self, Scope};
    let (ingress, _) = fixture().await;
    let (api, auth, _) = setup().await;
    let identity = api
        .foreground_admission()
        .unwrap()
        .authenticate(&headers("alice"))
        .await
        .unwrap();
    let scope = Scope::from_foreground_identity(identity).unwrap();
    let upstream = composio::tests::Fixture::with_scope(scope).await;
    let api = api
        .with_composio(Arc::new(upstream.gateway.clone()))
        .unwrap();
    let path = "/api/digitalmate/connectors/v1/composio/cancel";
    assert!(DigitalMateGatewayApi::matches(path));
    let request = |token: &str, value: &str| {
        Request::post(path)
            .header("content-type", "application/json")
            .header("authorization", token)
            .body(Full::new(Bytes::copy_from_slice(value.as_bytes())))
            .unwrap()
    };
    assert_eq!(
        api.connector_request(
            request(
                "Bearer alice-session",
                r#"{"registration":"registration-one","user_id":"bob"}"#
            ),
            ingress.clone()
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        api.connector_request(
            request(
                "Bearer bob-session",
                r#"{"registration":"registration-one"}"#
            ),
            ingress.clone()
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        api.connector_request(
            request(
                "Bearer alice-session",
                r#"{"registration":"registration-one"}"#
            ),
            ingress.clone()
        )
        .await
        .status(),
        StatusCode::OK
    );
    auth.0.store(true, Ordering::SeqCst);
    assert_eq!(
        api.connector_request(
            request(
                "Bearer alice-session",
                r#"{"registration":"registration-one"}"#
            ),
            ingress
        )
        .await
        .status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn foreground_composio_scope_carries_live_guard_without_becoming_json_identity() {
    use crate::ingress::digitalmategateway::composio::Scope;
    let (api, auth, _) = setup().await;
    let identity = api
        .foreground_admission()
        .unwrap()
        .authenticate(&headers("alice"))
        .await
        .unwrap();
    let scope = Scope::from_foreground_identity(identity).unwrap();
    scope.check_foreground().await.unwrap();
    auth.0.store(true, Ordering::SeqCst);
    assert_eq!(
        scope.check_foreground().await.err(),
        Some(crate::ingress::digitalmategateway::composio::Error::Forbidden)
    );
}

#[tokio::test]
async fn foreground_composio_revocation_during_shared_budget_wait_prevents_dispatch() {
    use crate::ingress::digitalmategateway::composio::{self, Budget, Error, Scope};
    struct RevokeBudget(Arc<TestAuth>);
    #[async_trait::async_trait]
    impl Budget for RevokeBudget {
        async fn acquire(&self, _: &Scope) -> Result<(), Error> {
            self.0 .0.store(true, Ordering::SeqCst);
            Ok(())
        }
    }
    let (api, auth, _) = setup().await;
    let identity = api
        .foreground_admission()
        .unwrap()
        .authenticate(&headers("alice"))
        .await
        .unwrap();
    let scope = Scope::from_foreground_identity(identity).unwrap();
    let mut upstream = composio::tests::Fixture::with_scope(scope.clone()).await;
    upstream.use_budget(Arc::new(RevokeBudget(auth)));
    assert_eq!(
        upstream
            .gateway
            .reconcile_authorization(&scope, "registration-one")
            .await
            .err(),
        Some(Error::Forbidden)
    );
    assert_eq!(upstream.call_count(), 0);
}

#[tokio::test]
async fn foreground_composio_routes_reach_real_ingress_dispatch_without_ws_or_model() {
    use crate::ingress::digitalmategateway::composio::{self, Scope};
    let (api, _, _) = setup().await;
    let identity = api
        .foreground_admission()
        .unwrap()
        .authenticate(&headers("alice"))
        .await
        .unwrap();
    let upstream =
        composio::tests::Fixture::with_scope(Scope::from_foreground_identity(identity).unwrap())
            .await;
    let api = api
        .with_composio(Arc::new(upstream.gateway.clone()))
        .unwrap();
    let (ingress, _) = fixture().await;
    let ingress = Arc::new(
        Arc::try_unwrap(ingress)
            .ok()
            .unwrap()
            .with_digitalmategateway_api(Arc::new(api)),
    );
    let (mut sender, server, client) = connection(ingress, IngressSecurity::Tls).await;
    let response = sender
        .send_request(
            Request::post("/api/digitalmate/connectors/v1/composio/cancel")
                .header("host", "localhost")
                .header("authorization", "Bearer alice-session")
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from_static(
                    br#"{"registration":"registration-one"}"#,
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&response.collect().await.unwrap().to_bytes()).unwrap(),
        json!({"phase":"cancelled"})
    );
    let response = sender
        .send_request(
            Request::get(
                "/api/digitalmate/connectors/v1/composio/callback?session_uri=opaque-must-not-echo",
            )
            .header("host", "localhost")
            .body(Full::new(Bytes::new()))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        !String::from_utf8_lossy(&response.collect().await.unwrap().to_bytes())
            .contains("opaque-must-not-echo")
    );
    assert_eq!(upstream.call_count(), 0);
    let response = sender
        .send_request(
            Request::post(composio::http::VALIDATE_PATH)
                .header("host", "localhost")
                .header("authorization", "Bearer alice-session")
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from_static(
                    br#"{"connection_id":"connection-one"}"#,
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let _ = response.collect().await.unwrap();
    assert_eq!(upstream.call_count(), 0);
    server.abort();
    client.abort();
}
