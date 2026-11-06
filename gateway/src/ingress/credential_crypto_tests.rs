//! Component evidence: real grant authority, Gateway, Relay, TLS and crypto router.
use super::*;
use crate::{
    common::protocol::GatewayPolicy,
    ingress::credential_crypto::{CredentialCryptoApi, CredentialCryptoConfig},
    node::Relay,
};
use adx_agent_api::{local::LocalControl, managed::ManagedService};
use adx_agent_core::{service_grants::*, Scope, TemplateDraft, TemplateRef};
use adx_credential_crypto::{
    access::{AccessClient, AccessConfig},
    service::{router, ServiceState},
    storage::write_private,
    Keyring,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use std::{collections::BTreeMap, time::Duration};
use tower::ServiceExt;

const ISSUER: &str = "private-crypto-test-issuer-at-least-32-bytes";
const VERIFIER: &str = "private-crypto-test-verifier-at-least-32-bytes";
const CERT: &[u8] = include_bytes!("../../tests/fixtures/ingress-cert.pem");
const KEY: &[u8] = include_bytes!("../../tests/fixtures/ingress-key.pem");

struct Fixture {
    gateway: Arc<Ingress>,
    backend: Arc<Backend>,
    activator: Arc<adx_activator::Activator>,
    grant: IssuedServiceGrant,
    headers: Arc<Mutex<Vec<http::HeaderMap>>>,
    stall: Arc<std::sync::atomic::AtomicBool>,
    authority_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    _directory: tempfile::TempDir,
}
impl Fixture {
    async fn post(&self, path: &str, body: Value) -> (StatusCode, Value) {
        send(
            self,
            IngressSecurity::Tls,
            "POST",
            path,
            &self.grant.credential,
            body,
        )
        .await
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}
fn scope(template: &str, binding: &str) -> Scope {
    Scope {
        tenant: "tenant".into(),
        template: template.into(),
        template_ref: TemplateRef::Version("1".into()),
        binding_id: binding.into(),
    }
}
fn context() -> Value {
    json!({"tenant":"tenant", "workspace":"alice", "purpose":"orbit-config"})
}
fn seal() -> Value {
    json!({"context":context(), "plaintext":STANDARD.encode(b"provider-secret")})
}
async fn fixture_crypto(server_name: &str, timeout_seconds: u64) -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let token_file = directory.path().join("verification");
    let ca_file = directory.path().join("ca.pem");
    write_private(&token_file, VERIFIER.as_bytes(), false).unwrap();
    write_private(&ca_file, CERT, false).unwrap();
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let backend = Arc::new(Backend::default());
    let activator = Arc::new(adx_activator::Activator::new(
        state.clone(),
        backend.clone(),
    ));
    let authority = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", authority.local_addr().unwrap());
    let app = adx_activator::server::router(activator.clone(), ISSUER, Duration::from_secs(3))
        .unwrap()
        .merge(
            adx_activator::service_grants::router(
                activator.clone(),
                ISSUER,
                VERIFIER,
                Duration::from_secs(3),
            )
            .unwrap(),
        );
    let (authority_shutdown, shutdown) = tokio::sync::oneshot::channel();
    let authority_task = tokio::spawn(async move {
        axum::serve(authority, app)
            .with_graceful_shutdown(async {
                let _ = shutdown.await;
            })
            .await
            .unwrap()
    });
    let access = || AccessConfig {
        url: url.clone(),
        token_file: token_file.clone(),
        ca_file: None,
        allow_loopback_http: true,
        timeout_seconds: 1,
    };
    let app = router(ServiceState {
        keys: Keyring::new("v1".into(), BTreeMap::from([("v1".into(), [7; 32])])).unwrap(),
        authorizer: AccessClient::new(&access()).unwrap(),
    });
    let private_key = directory.path().join("tls-key.pem");
    write_private(&private_key, KEY, false).unwrap();
    let acceptor = adx_transport::tls::http_server_acceptor(
        &ca_file,
        &private_key,
        None,
        vec![b"http/1.1".to_vec()],
    )
    .unwrap();
    let crypto = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = crypto.local_addr().unwrap().port();
    let headers = Arc::new(Mutex::new(Vec::new()));
    let observed = headers.clone();
    let stall = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stalled = stall.clone();
    let crypto_task = tokio::spawn(async move {
        loop {
            let (stream, _) = crypto.accept().await.unwrap();
            let acceptor = acceptor.clone();
            let app = app.clone();
            let observed = observed.clone();
            let stalled = stalled.clone();
            tokio::spawn(async move {
                let Ok(tls) = acceptor.accept(stream).await else {
                    return;
                };
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(
                        TokioIo::new(tls),
                        service_fn(move |request: Request<Incoming>| {
                            observed.lock().unwrap().push(request.headers().clone());
                            let app = app.clone();
                            let stall = stalled.load(std::sync::atomic::Ordering::SeqCst);
                            async move {
                                if stall {
                                    tokio::time::sleep(Duration::from_secs(10)).await;
                                }
                                app.oneshot(request.map(axum::body::Body::new)).await
                            }
                        }),
                    )
                    .await;
            });
        }
    });
    let mut template: TemplateDraft = serde_json::from_str(include_str!(
        "../../../agent/activator/examples/credential-crypto-template.json"
    ))
    .unwrap();
    template.service[0].port = port;
    template
        .readiness_probe
        .as_mut()
        .unwrap()
        .tcp_socket
        .as_mut()
        .unwrap()
        .port = port;
    template.validate().unwrap();
    state.publish("tenant", &template).await.unwrap();
    let user_template: TemplateDraft = serde_json::from_value(json!({
        "name":"orbit", "image":"app:1", "isolation_runtime":"runsc",
        "resources":{"cpu_millis":1000,"memory_mib":512}
    }))
    .unwrap();
    state.publish("tenant", &user_template).await.unwrap();
    let user = state
        .create_binding_with_parameters(
            scope("orbit", "alice"),
            &BTreeMap::from([("workspace".into(), "alice".into())]),
        )
        .await
        .unwrap();
    let grant = activator
        .issue_service_grant(&IssueServiceGrant {
            scope: user.scope,
            expected_generation: user.generation,
            purpose: ORBIT_CONFIG_PURPOSE.into(),
            ttl_seconds: 60,
        })
        .await
        .unwrap();
    let binding = state
        .create_binding(scope("credential-crypto", "shared"))
        .await
        .unwrap();
    let relay = Arc::new(
        Relay::new(GatewayPolicy::for_local_mock(vec!["127.0.0.0/8"
            .parse()
            .unwrap()]))
        .with_route_enforcement(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let relay_server = relay.clone();
    let relay_task = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let relay = relay_server.clone();
            tokio::spawn(async move {
                let _ = relay.serve_h2(stream).await;
            });
        }
    });
    let (gateway, _, routes) = fixture_with_routes().await;
    let route: crate::common::route::RouteInfo = serde_json::from_value(json!({
        "instanceID":binding.current.environment_id,"instanceStatus":{"code":3},
        "tenantID":"tenant", "environmentID":"crypto-runtime", "environmentIP":"127.0.0.1",
        "nodeProxyAddress":address.to_string()
    }))
    .unwrap();
    relay
        .activate_route(
            route.instance_id.clone(),
            route.environment_id.clone(),
            route.environment_ip.parse().unwrap(),
        )
        .await;
    routes.put(route);
    let managed = Arc::new(ManagedService::new(Arc::new(
        adx_agent_api::activator::ActivatorClient::new(
            vec![url.clone()],
            ISSUER.into(),
            Duration::from_secs(5),
            None,
            true,
        )
        .unwrap(),
    )));
    let api = CredentialCryptoApi::new(
        CredentialCryptoConfig {
            scope: binding.scope,
            port,
            server_name: server_name.into(),
            ca_file,
            timeout_seconds,
        },
        managed,
    )
    .unwrap();
    let gateway = Arc::new(
        gateway
            .as_ref()
            .clone()
            .with_credential_crypto_api(Arc::new(api)),
    );
    Fixture {
        gateway,
        backend,
        activator,
        grant,
        headers,
        stall,
        authority_shutdown: Some(authority_shutdown),
        tasks: vec![authority_task, crypto_task, relay_task],
        _directory: directory,
    }
}
async fn send(
    f: &Fixture,
    security: IngressSecurity,
    method: &str,
    path: &str,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    let (mut sender, server, client) = connection(f.gateway.clone(), security).await;
    let mut request = request(method, path, token, "crypto-test", body);
    for name in ["x-adx-target-port", "x-adx-instance-id", "x-auth", "cookie"] {
        request
            .headers_mut()
            .insert(name, "attacker-controlled".parse().unwrap());
    }
    let response = sender.send_request(request).await.unwrap();
    let status = response.status();
    let body = response.collect().await.unwrap().to_bytes();
    let body = serde_json::from_slice(&body)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&body).into()));
    server.abort();
    client.abort();
    (status, body)
}

#[tokio::test]
async fn managed_crypto_roundtrip_uses_fixed_binding_tls_and_only_workspace_identity() {
    let f = fixture_crypto("localhost", 5).await;
    assert!(f.backend.0.lock().unwrap().is_empty());
    let (status, sealed) = f.post("/v1/seal", seal()).await;
    assert_eq!(status, StatusCode::OK, "{sealed}");
    let (status, opened) = f
        .post(
            "/v1/open",
            json!({"context":context(), "envelope":sealed["envelope"]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{opened}");
    assert_eq!(opened["plaintext"], STANDARD.encode(b"provider-secret"));
    assert_eq!(
        f.gateway.backend_http_pool_metrics().opened_total,
        2,
        "crypto must use the shared backend HTTP transport"
    );
    assert_eq!(
        f.gateway.backend_http_pool_metrics().idle_connections,
        0,
        "crypto requests must not retain a connection across service identities"
    );
    assert_eq!(f.backend.0.lock().unwrap().len(), 1);
    let headers = f.headers.lock().unwrap();
    assert_eq!(headers.len(), 2);
    for headers in headers.iter() {
        assert_eq!(
            headers["authorization"],
            format!("Bearer {}", f.grant.credential)
        );
        assert_eq!(headers["content-type"], "application/json");
        for name in [
            "x-adx-target-port",
            "x-adx-instance-id",
            "x-auth",
            "cookie",
            "x-trace-id",
        ] {
            assert!(!headers.contains_key(name), "leaked {name}");
        }
    }
}

#[tokio::test]
async fn denied_admission_never_activates_crypto() {
    let f = fixture_crypto("localhost", 5).await;
    for (security, method, path, token, expected) in [
        (
            IngressSecurity::Plaintext,
            "POST",
            "/v1/seal",
            f.grant.credential.as_str(),
            StatusCode::UPGRADE_REQUIRED,
        ),
        (
            IngressSecurity::Tls,
            "GET",
            "/v1/seal",
            f.grant.credential.as_str(),
            StatusCode::METHOD_NOT_ALLOWED,
        ),
        (
            IngressSecurity::Tls,
            "POST",
            "/v1/seal?target=other",
            f.grant.credential.as_str(),
            StatusCode::BAD_REQUEST,
        ),
        (
            IngressSecurity::Tls,
            "POST",
            "/v1/seal",
            "tenant",
            StatusCode::UNAUTHORIZED,
        ),
        (
            IngressSecurity::Tls,
            "POST",
            "/v1/seal",
            "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            StatusCode::FORBIDDEN,
        ),
    ] {
        let (status, body) = send(&f, security, method, path, token, seal()).await;
        assert_eq!(status, expected, "{method} {path}: {body}");
    }
    for (field, value) in [
        ("workspace", "bob"),
        ("tenant", "other"),
        ("purpose", "digitalmate-config"),
    ] {
        let mut body = seal();
        body["context"][field] = json!(value);
        assert_eq!(
            f.post("/v1/seal", body).await.0,
            StatusCode::FORBIDDEN,
            "cross-{field}"
        );
    }
    assert!(f.backend.0.lock().unwrap().is_empty());
    assert!(f.headers.lock().unwrap().is_empty());
}

#[tokio::test]
async fn revoked_grant_or_authority_outage_denies_before_activation() {
    let mut f = fixture_crypto("localhost", 5).await;
    f.activator
        .revoke_service_grant(&RevokeServiceGrant {
            credential_digest: f.grant.credential_digest.clone(),
        })
        .await
        .unwrap();
    assert_eq!(f.post("/v1/seal", seal()).await.0, StatusCode::FORBIDDEN);
    f.authority_shutdown.take().unwrap().send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), &mut f.tasks[0])
        .await
        .unwrap()
        .unwrap();
    let (_, body) = f.post("/v1/seal", seal()).await;
    assert_eq!(body["code"], "AUTHORITY_UNAVAILABLE");
    assert!(f.backend.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn managed_crypto_deadline_closes_stalled_exchange_without_replaying_request() {
    let f = fixture_crypto("localhost", 1).await;
    f.stall.store(true, std::sync::atomic::Ordering::SeqCst);
    let start = std::time::Instant::now();
    let (status, body) = f.post("/v1/seal", seal()).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["code"], "CRYPTO_UNAVAILABLE");
    assert!(start.elapsed() < Duration::from_secs(3));
    assert_eq!(
        f.headers.lock().unwrap().len(),
        1,
        "unknown outcome must not replay"
    );
    tokio::time::timeout(Duration::from_millis(500), async {
        while f.gateway.active_sessions() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("deadline must release the shared Relay session");
}

#[tokio::test]
async fn managed_crypto_rejects_untrusted_backend_tls_without_forwarding_plaintext() {
    let f = fixture_crypto("wrong-server.example", 5).await;
    let (status, body) = f.post("/v1/seal", seal()).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["code"], "CRYPTO_UNAVAILABLE");
    assert!(f.headers.lock().unwrap().is_empty());
}

#[tokio::test]
async fn managed_crypto_bounds_bodies_and_rejects_upgrade_and_ambiguous_auth() {
    let f = fixture_crypto("localhost", 5).await;
    let body = json!({"context":context(),"plaintext":"A".repeat(524_288)});
    assert_eq!(
        f.post("/v1/seal", body).await.0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    for header in ["authorization", "upgrade"] {
        let (mut sender, server, client) =
            connection(f.gateway.clone(), IngressSecurity::Tls).await;
        let mut r = request("POST", "/v1/seal", &f.grant.credential, "crypto", seal());
        r.headers_mut().append(header, "untrusted".parse().unwrap());
        let response = sender.send_request(r).await.unwrap();
        assert!(response.status().is_client_error());
        response.collect().await.unwrap();
        server.abort();
        client.abort();
    }
    assert!(f.backend.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn managed_crypto_configuration_fails_closed_before_serving() {
    let f = fixture_crypto("localhost", 5).await;
    let managed = Arc::new(ManagedService::new(Arc::new(LocalControl::new(
        f.activator.clone(),
    ))));
    let value = json!({
        "scope":scope("credential-crypto", "shared"), "port":18443, "server_name":"localhost",
        "ca_file":f._directory.path().join("ca.pem"), "timeout_seconds":60
    });
    assert!(CredentialCryptoApi::new(
        serde_json::from_value(value.clone()).unwrap(),
        managed.clone()
    )
    .is_ok());
    for (field, invalid) in [
        ("port", json!(0)),
        ("timeout_seconds", json!(0)),
        ("timeout_seconds", json!(301)),
        ("server_name", json!("https://localhost/path")),
        (
            "scope",
            json!({"tenant":"", "template":"crypto", "template_ref":{"version":"1"},"binding_id":"shared"}),
        ),
        ("ca_file", json!(f._directory.path().join("verification"))),
    ] {
        let mut invalid_config = value.clone();
        invalid_config[field] = invalid;
        assert!(
            CredentialCryptoApi::new(
                serde_json::from_value(invalid_config).unwrap(),
                managed.clone()
            )
            .is_err(),
            "accepted invalid {field}"
        );
    }
}
