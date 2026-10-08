//! Transport and fixed-ID mapping checks; no live Platform claim.
use super::*;
use crate::ingress::{
    agent_api::AgentApi,
    auth::{AuthError, AuthenticatedIdentity, CredentialVerifier},
    inline_api::{InlineApi, InlineConfig},
    H2PoolConfig, RouteStore,
};
use adx_agent_api::management::InlineProfile;
use adx_agent_core::{
    inline::{CreateRequest, SandboxType},
    sandbox::*,
};
use adx_agent_store::{AgentState, MemoryRepository};

#[derive(Default)]
struct Backend(
    Mutex<std::collections::HashMap<(String, String), SandboxObservation>>,
    std::sync::atomic::AtomicUsize,
);
#[async_trait::async_trait]
impl Sandbox for Backend {
    async fn list(&self, tenant: &str) -> Result<Vec<SandboxInfo>, SandboxError> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .values()
            .filter(|v| v.tenant == tenant && v.phase == SandboxPhase::Running)
            .cloned()
            .map(|observation| SandboxInfo {
                observation,
                node_ip: None,
                sandbox_ip: None,
                execution: None,
            })
            .collect())
    }

    async fn create(&self, r: &CreateSandbox) -> Result<SandboxObservation, SandboxError> {
        let observed = SandboxObservation {
            id: r.id.clone(),
            tenant: r.tenant.clone(),
            phase: SandboxPhase::Running,
            ready: true,
            runtime_id: Some(format!("{}-runtime", r.id)),
            message: None,
        };
        self.0
            .lock()
            .unwrap()
            .insert((r.tenant.clone(), r.id.clone()), observed.clone());
        Ok(observed)
    }
    async fn get(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<SandboxObservation>, SandboxError> {
        self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let entries = self.0.lock().unwrap();
        let observed = entries.get(&(tenant.into(), id.into())).cloned();
        if observed.is_none() && entries.keys().any(|(_, instance)| instance == id) {
            return Err(SandboxError::NotFound);
        }
        Ok(observed)
    }
    async fn delete(&self, tenant: &str, id: &str) -> Result<SandboxObservation, SandboxError> {
        let mut entries = self.0.lock().unwrap();
        if !entries.contains_key(&(tenant.into(), id.into()))
            && entries.keys().any(|(_, instance)| instance == id)
        {
            return Err(SandboxError::NotFound);
        }
        let value = entries
            .get_mut(&(tenant.into(), id.into()))
            .ok_or_else(|| SandboxError::OutcomeUnknown("deletion not confirmed".into()))?;
        value.phase = SandboxPhase::Deleted;
        value.ready = false;
        Ok(value.clone())
    }
}
#[derive(Debug)]
struct Identity;
#[async_trait::async_trait]
impl CredentialVerifier for Identity {
    async fn verify(&self, key: &str) -> Result<AuthenticatedIdentity, AuthError> {
        match key {
            "tenant" | "other" => Ok(AuthenticatedIdentity {
                tenant_id: key.into(),
                expires_at_unix: None,
            }),
            _ => Err(AuthError::Invalid("test credential".into())),
        }
    }
}
async fn fixture() -> (Arc<Ingress>, Arc<InlineApi>) {
    let (gateway, api, _) = fixture_with_routes().await;
    (gateway, api)
}
async fn fixture_with_routes() -> (Arc<Ingress>, Arc<InlineApi>, Arc<RouteStore>) {
    fixture_with_backend(Arc::new(Backend::default())).await
}
async fn fixture_with_backend(
    backend: Arc<dyn Sandbox>,
) -> (Arc<Ingress>, Arc<InlineApi>, Arc<RouteStore>) {
    let api = Arc::new(
        InlineApi::new(
            InlineConfig {
                inline_profiles: vec![InlineProfile {
                    sandbox_type: SandboxType::Docker,
                    request_image: Some("app:1".into()),
                    image: "app:1".into(),
                    isolation_runtime: "runc".into(),
                    request_user: None,
                    working_dir: "/".into(),
                    default_entrypoint: vec![],
                    service: vec![],
                    preinstalled_workspace: None,
                    preinstalled_mounts: vec![],
                }],
                backend_timeout_seconds: 1,
                max_inflight: 16,
                iam_address: iam_server().await,
            },
            backend,
        )
        .unwrap(),
    );
    let store = Arc::new(RouteStore::new());
    store.set_ready(true);
    let gateway = Arc::new(
        Ingress::new(
            Arc::new(IngressRouteResolver::new(store.clone())),
            DataPlaneL4Connector::new(H2PoolConfig::default()),
            IngressAuthenticator::with_verifier(Arc::new(Identity)),
            50090,
            8765,
            "127.0.0.1:1",
            vec![],
        )
        .with_inline_api(api.clone()),
    );
    (gateway, api, store)
}
fn input() -> serde_json::Value {
    serde_json::json!({"name":"demo","namespace":"default","runtime_spec":{"runtime":"Python3.11","sandbox_type":"docker","rootfs":{"imageurl":"app:1"}}})
}
async fn connection(
    gateway: Arc<Ingress>,
    security: IngressSecurity,
) -> (
    hyper::client::conn::http1::SendRequest<Full<Bytes>>,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), hyper::Error>>,
) {
    let (client, server) = tokio::io::duplex(64 * 1024);
    let serving = tokio::spawn(async move {
        hyper::server::conn::http1::Builder::new()
            .serve_connection(
                TokioIo::new(server),
                service_fn(move |request| {
                    gateway.clone().handle_http(
                        request,
                        security,
                        "127.0.0.1:10000".parse().unwrap(),
                    )
                }),
            )
            .with_upgrades()
            .await
            .unwrap();
    });
    let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(client))
        .await
        .unwrap();
    (sender, serving, tokio::spawn(connection.with_upgrades()))
}
fn request(
    method: &str,
    path: &str,
    token: &str,
    trace: &str,
    body: serde_json::Value,
) -> Request<Full<Bytes>> {
    let inline = InlineApi::matches(path.split('?').next().unwrap());
    let builder = Request::builder()
        .method(method)
        .uri(path)
        .header("x-trace-id", trace);
    let builder = if inline && matches!(token, "tenant" | "other") {
        builder.header("x-auth", inline_token(token, "developer", "signature"))
    } else {
        builder.header("authorization", format!("Bearer {token}"))
    };
    builder
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap()
}
async fn json(response: Response<Incoming>) -> serde_json::Value {
    serde_json::from_slice(&response.collect().await.unwrap().to_bytes()).unwrap()
}

#[tokio::test]
async fn legacy_inline_http_contract_and_tenant_boundary() {
    let (gateway, _) = fixture().await;
    assert!(gateway.ready());
    let (mut sender, server, client) = connection(gateway, IngressSecurity::Tls).await;
    let response = sender
        .send_request(request(
            "POST",
            "/api/agent",
            "tenant",
            "create-trace",
            input(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-trace-id"], "create-trace");
    let created = json(response).await;
    assert_eq!(created["code"], 200);
    let id = created["instance_id"].as_str().unwrap();
    assert!(uuid::Uuid::parse_str(id).is_ok());
    let path = format!("/api/agent/{id}");
    let response = sender
        .send_request(request(
            "GET",
            &path,
            "tenant",
            "get-trace",
            serde_json::Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(response.headers()["x-trace-id"], "get-trace");
    let detail = json(response).await;
    assert_eq!(detail["instance"]["instance_id"], id);
    assert_eq!(detail["instance"]["status"], "RUNNING");
    let response = sender
        .send_request(request(
            "GET",
            &path,
            "other",
            "other-trace",
            serde_json::Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(json(response).await["error"]["kind"], "not_found");
    let response = sender
        .send_request(request(
            "DELETE",
            &path,
            "other",
            "denied-kill",
            serde_json::Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(json(response).await["error"]["kind"], "not_found");
    let response = sender
        .send_request(request(
            "GET",
            &path,
            "tenant",
            "still-running",
            serde_json::Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json(response).await["instance"]["status"], "RUNNING");
    let response = sender
        .send_request(request(
            "DELETE",
            &path,
            "tenant",
            "kill-trace",
            serde_json::Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-trace-id"], "kill-trace");
    assert_eq!(
        json(response).await,
        serde_json::json!({"code":200,"status":"deleted"})
    );
    let response = sender
        .send_request(request(
            "DELETE",
            &path,
            "tenant",
            "again",
            serde_json::Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    json(response).await;
    client.abort();
    server.abort();
}
#[tokio::test]
async fn inline_http_rejects_plaintext_bad_credentials_and_invalid_inputs() {
    let (gateway, _) = fixture().await;
    let (mut sender, server, client) =
        connection(gateway.clone(), IngressSecurity::Plaintext).await;
    let response = sender
        .send_request(request("POST", "/api/agent", "tenant", "trace", input()))
        .await
        .unwrap();
    assert!(!response.status().is_success());
    response.collect().await.unwrap();
    client.abort();
    server.abort();
    let (mut sender, server, client) = connection(gateway, IngressSecurity::Tls).await;
    for (token, trace, body, status) in [
        ("bad", "trace", input(), StatusCode::UNAUTHORIZED),
        ("tenant", " ", input(), StatusCode::BAD_REQUEST),
        (
            "tenant",
            "trace",
            serde_json::json!({"name":"no-runtime"}),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let response = sender
            .send_request(request("POST", "/api/agent", token, trace, body))
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        response.collect().await.unwrap();
    }
    client.abort();
    server.abort();
}
#[tokio::test]
async fn inline_sandbox_identity_is_preserved_for_http_ws_and_connect() {
    let (gateway, api) = fixture().await;
    let request: CreateRequest = serde_json::from_value(input()).unwrap();
    let id = api
        .service
        .create(
            &adx_agent_api::request::RequestContext::new(std::time::Duration::from_secs(60)),
            "tenant",
            request,
        )
        .await
        .unwrap()
        .instance_id;
    for path in [
        format!("/{id}/8080/a?next={id}"),
        format!("/direct/{id}/v1?x=1"),
        format!("/tunnel/{id}/2222"),
    ] {
        let mut request = Request::builder()
            .uri(&path)
            .header("authorization", "Bearer tenant")
            .body(())
            .unwrap();
        gateway
            .prepare_agent_data(&mut request, IngressSecurity::Tls)
            .await
            .unwrap();
        assert_eq!(request.uri().to_string(), path);
    }
    let mut request = Request::builder()
        .method("CONNECT")
        .uri(format!("{id}:2222"))
        .header("host", format!("{id}:2222"))
        .header("authorization", "Bearer tenant")
        .body(())
        .unwrap();
    gateway
        .prepare_agent_data(&mut request, IngressSecurity::Tls)
        .await
        .unwrap();
    assert_eq!(request.uri().to_string(), format!("{id}:2222"));
    assert_eq!(request.headers()["host"], format!("{id}:2222"));
    // Forwarding state is owned by Platform; this adapter does not inspect inline lifecycle.
    api.service.kill("tenant", &id).await.unwrap();
}

async fn remote_control(
    inner: adx_activator::Activator,
) -> adx_agent_api::activator::ActivatorClient {
    let token = "gateway-test-activator-token-at-least-32-bytes";
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let timeout = adx_agent_core::limits::AGENT_REQUEST_TIMEOUT;
    let app = adx_activator::server::router(Arc::new(inner), token, timeout).unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    adx_agent_api::activator::ActivatorClient::new(vec![url], token.into(), timeout, None, true)
        .unwrap()
}
fn control_context() -> adx_agent_api::request::RequestContext {
    adx_agent_api::request::RequestContext::new(adx_agent_core::limits::AGENT_REQUEST_TIMEOUT)
}

#[tokio::test]
async fn agent_configuration_builds_client_and_validates_deadline() {
    use crate::ingress::agent_api::AgentConfig;
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    // Read an existing test process variable without mutating global environment.
    let settings = serde_json::json!({
        "activator":{"urls":[url], "token_env":"PATH", "allow_plaintext":true}
    });
    let config: AgentConfig = serde_json::from_value(settings.clone()).unwrap();
    assert_eq!(config.timeout_seconds, 60);
    let api = AgentApi::new(config).unwrap();
    assert!(matches!(
        api.managed
            .template(&control_context(), "tenant", "demo", "1")
            .await,
        Err(adx_agent_api::Error::Unavailable(_))
    ));
    for timeout in [0, u64::MAX] {
        let mut value = settings.clone();
        value["timeout_seconds"] = serde_json::json!(timeout);
        let config = serde_json::from_value(value).unwrap();
        let error = AgentApi::new(config).err().unwrap();
        assert!(matches!(
            error.downcast_ref::<adx_agent_api::Error>(),
            Some(adx_agent_api::Error::Invalid(_))
        ));
    }
}

#[tokio::test]
#[ignore = "requires ADX_AGENT_TEST_REDIS_URL pointing to a disposable Redis"]
async fn independent_activators_share_redis_identity() {
    let redis_url = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let namespace = format!("remote-{}", uuid::Uuid::new_v4());
    let backend = Arc::new(Backend::default());
    let mut clients = Vec::new();
    for _ in 0..2 {
        let store = adx_agent_store::RedisRepository::connect(
            &redis_url,
            &namespace,
            std::time::Duration::from_secs(3),
        )
        .await
        .unwrap();
        let client = remote_control(adx_activator::Activator::new(
            AgentState::new(Arc::new(store)),
            backend.clone(),
        ))
        .await;
        clients.push(AgentApi {
            managed: Arc::new(adx_agent_api::managed::ManagedService::new(Arc::new(
                client,
            ))),
            request_timeout: adx_agent_core::limits::AGENT_REQUEST_TIMEOUT,
        });
    }
    let a = &clients[0];
    let b = &clients[1];
    let template = serde_json::from_value(serde_json::json!({"name":"demo","version":"1","image":"app:1","isolation_runtime":"runc","entrypoint":["/start"],"resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080}]})).unwrap();
    a.managed
        .publish(&control_context(), "tenant", &template)
        .await
        .unwrap();
    let scope = adx_agent_core::Scope {
        tenant: "tenant".into(),
        template: "demo".into(),
        version: "1".into(),
        environment_id: "env".into(),
    };
    let ctx = control_context();
    let (left, right) = tokio::join!(
        a.managed
            .resolve(&ctx, &scope, adx_agent_core::Protocol::Http, None),
        b.managed
            .resolve(&ctx, &scope, adx_agent_core::Protocol::Http, None)
    );
    let original = left.unwrap();
    assert_eq!(original, right.unwrap());
    assert_eq!(backend.0.lock().unwrap().len(), 1);
    b.managed
        .delete_environment(&control_context(), &scope)
        .await
        .unwrap();
    let fresh = a
        .managed
        .resolve(&ctx, &scope, adx_agent_core::Protocol::Http, None)
        .await
        .unwrap();
    assert_ne!(
        original.0.environment.generation,
        fresh.0.environment.generation
    );
    assert_ne!(
        original.0.environment.sandbox_id,
        fresh.0.environment.sandbox_id
    );
    a.managed
        .delete_environment(&control_context(), &scope)
        .await
        .unwrap();
}
#[tokio::test]
async fn managed_environment_management_and_protocol_forwarding() {
    let (gateway, _) = fixture().await;
    let backend = Arc::new(Backend::default());
    let control = Arc::new(
        remote_control(adx_activator::Activator::new(
            AgentState::new(Arc::new(MemoryRepository::default())),
            backend.clone(),
        ))
        .await,
    );
    let api = Arc::new(AgentApi {
        managed: Arc::new(adx_agent_api::managed::ManagedService::new(control)),
        request_timeout: adx_agent_core::limits::AGENT_REQUEST_TIMEOUT,
    });
    let gateway = Arc::new(
        Arc::try_unwrap(gateway)
            .ok()
            .unwrap()
            .with_agent_api(api.clone()),
    );
    let (mut sender, server, client) = connection(gateway.clone(), IngressSecurity::Tls).await;
    let template = serde_json::json!({"name":"demo","version":"1","image":"app:1","isolation_runtime":"runc","entrypoint":["/start"],"resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080},{"protocol":"ws","port":8080},{"protocol":"ssh","port":22}]});
    let response = sender
        .send_request(request(
            "POST",
            "/api/agent/v2/templates",
            "tenant",
            "publish",
            template,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    json(response).await;
    let path = "/api/agent/v2/templates/demo/versions/1/environments/env";
    let mut traffic = Request::builder()
        .uri("/agent/http/path?target=urn:adx:environment:demo:1:env&port=8080")
        .header("authorization", "Bearer tenant")
        .body(())
        .unwrap();
    gateway
        .prepare_agent_data(&mut traffic, IngressSecurity::Tls)
        .await
        .unwrap();
    let response = sender
        .send_request(request(
            "GET",
            path,
            "tenant",
            "query",
            serde_json::json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let environment = json(response).await["environment"].clone();
    let listing = "/api/agent/v2/templates/demo/versions/1/environments";
    let response = sender
        .send_request(request(
            "GET",
            &format!("{listing}?page_size=1"),
            "tenant",
            "list",
            serde_json::json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let page = json(response).await;
    assert_eq!(
        page["environments"],
        serde_json::json!([environment.clone()])
    );
    assert!(page["next_page_token"].is_null());
    for query in [
        "page_size=0",
        "page_size=1&page_size=2",
        "tenant=other",
        "page_token=invalid",
    ] {
        let response = sender
            .send_request(request(
                "GET",
                &format!("{listing}?{query}"),
                "tenant",
                "invalid-list",
                serde_json::json!({}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        json(response).await;
    }
    let id = environment["sandbox_id"].as_str().unwrap();
    let response = sender
        .send_request(request(
            "POST",
            &format!("{path}/resolve"),
            "tenant",
            "ssh",
            serde_json::json!({"protocol":"ssh"}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let resolved = json(response).await;
    assert_eq!(resolved["sandbox_id"], id);
    assert_eq!(resolved["port"], 22);
    assert_eq!(
        backend.1.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "default resolve reuses successful activation"
    );
    let response = sender
        .send_request(request(
            "POST",
            &format!("{path}/resolve"),
            "tenant",
            "refresh",
            serde_json::json!({"protocol":"http", "bypasscache":true}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json(response).await["sandbox_id"], id);
    assert_eq!(
        backend.1.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "public bypasscache reaches Sandbox GET"
    );
    let response = sender
        .send_request(request(
            "POST",
            &format!("{path}/resolve"),
            "tenant",
            "invalid-refresh",
            serde_json::json!({"protocol":"http", "bypasscache":"true"}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    json(response).await;
    for protocol in ["http", "ws"] {
        let mut builder = Request::builder()
            .uri(format!(
                "/agent/{protocol}/path?target=urn:adx:environment:demo:1:env&port=8080&q=1"
            ))
            .header("authorization", "Bearer tenant");
        if protocol == "ws" {
            builder = builder.header("upgrade", "websocket");
        }
        let mut data = builder.body(()).unwrap();
        gateway
            .prepare_agent_data(&mut data, IngressSecurity::Tls)
            .await
            .unwrap();
        assert_eq!(data.uri().to_string(), format!("/{id}/8080/path?q=1"));
        let before = backend.1.load(std::sync::atomic::Ordering::SeqCst);
        assert!(api.retry_data(&mut data).await.unwrap());
        assert_eq!(
            backend.1.load(std::sync::atomic::Ordering::SeqCst),
            before + 1,
            "managed retry bypasses activation cache"
        );
        assert!(!api.retry_data(&mut data).await.unwrap());
    }
    let mut pending = Request::builder()
        .uri("/agent/http/path?target=urn:adx:environment:demo:1:env&port=8080")
        .header("authorization", "Bearer tenant")
        .body(())
        .unwrap();
    gateway
        .prepare_agent_data(&mut pending, IngressSecurity::Tls)
        .await
        .unwrap();
    let response = sender
        .send_request(request(
            "DELETE",
            path,
            "tenant",
            "delete",
            serde_json::json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    json(response).await;
    assert!(matches!(
        api.retry_data(&mut pending).await,
        Err(adx_agent_api::Error::Conflict(_))
    ));
    let response = sender
        .send_request(request(
            "GET",
            path,
            "tenant",
            "read",
            serde_json::json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    json(response).await;
    server.abort();
    client.abort();
}

#[tokio::test]
async fn uuid_platform_routes_keep_their_existing_access_policy() {
    let (gateway, api, routes) = fixture_with_routes().await;
    // Even an actual Agent ID must not shadow a literal Platform route.
    let id = api
        .service
        .create(
            &adx_agent_api::request::RequestContext::new(std::time::Duration::from_secs(60)),
            "tenant",
            serde_json::from_value(input()).unwrap(),
        )
        .await
        .unwrap()
        .instance_id;
    routes.put(
        serde_json::from_value(serde_json::json!({
            "instanceID":id,"instanceStatus":{"code":3},"tenantID":"tenant",
            "sandboxID":"literal-runtime","sandboxIP":"127.0.0.1","nodeProxyAddress":"127.0.0.1:1"
        }))
        .unwrap(),
    );
    for security in [IngressSecurity::Plaintext, IngressSecurity::Tls] {
        for kind in ["tunnel", "port-forwarding", "ssh"] {
            for token in [None, Some("Bearer tenant"), Some("Bearer invalid")] {
                let mut builder = Request::builder()
                    .method("CONNECT")
                    .uri(format!("{id}:22"))
                    .header("host", format!("{id}:22"))
                    .header("x-adx-access-kind", kind);
                if let Some(token) = token {
                    builder = builder.header("authorization", token);
                }
                let mut request = builder.body(()).unwrap();
                let original = request.uri().clone();
                gateway
                    .prepare_agent_data(&mut request, security)
                    .await
                    .unwrap();
                assert_eq!(request.uri(), &original);
                assert!(request
                    .extensions()
                    .get::<AuthenticatedIdentity>()
                    .is_none());
            }
        }
        for prefix in ["tunnel", "port-forwarding"] {
            let mut request = Request::builder()
                .uri(format!("/{prefix}/{id}/22"))
                .body(())
                .unwrap();
            let original = request.uri().clone();
            gateway
                .prepare_agent_data(&mut request, security)
                .await
                .unwrap();
            assert_eq!(request.uri(), &original);
        }
    }
    // Unknown UUIDs also do not imply Agent TLS/authentication requirements.
    let mut request = Request::builder()
        .uri(format!("/tunnel/{}/22", uuid::Uuid::new_v4()))
        .body(())
        .unwrap();
    gateway
        .prepare_agent_data(&mut request, IngressSecurity::Plaintext)
        .await
        .unwrap();
}

#[derive(Debug)]
struct CountIdentity(std::sync::atomic::AtomicUsize);
#[async_trait::async_trait]
impl CredentialVerifier for CountIdentity {
    async fn verify(&self, key: &str) -> Result<AuthenticatedIdentity, AuthError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Identity.verify(key).await
    }
}
#[tokio::test]
async fn only_managed_data_entrypoint_resolves_and_reuses_authenticated_identity() {
    let (gateway, _) = fixture().await;
    let mut gateway = Arc::try_unwrap(gateway).ok().unwrap();
    let verifier = Arc::new(CountIdentity(std::sync::atomic::AtomicUsize::new(0)));
    gateway.authenticator = IngressAuthenticator::with_verifier(verifier.clone());
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let scope = adx_agent_core::Scope {
        tenant: "tenant".into(),
        template: "test".into(),
        version: "1".into(),
        environment_id: "env".into(),
    };
    let template = serde_json::from_value(serde_json::json!({"name":"test","version":"1","image":"app:1","isolation_runtime":"runc","entrypoint":["/start"],"resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080}]})).unwrap();
    state.publish("tenant", &template).await.unwrap();
    state.create_environment(scope.clone()).await.unwrap();
    let id = state.environment(&scope).await.unwrap().unwrap().sandbox_id;
    let control = Arc::new(
        remote_control(adx_activator::Activator::new(
            state.clone(),
            Arc::new(Backend::default()),
        ))
        .await,
    );
    gateway = gateway.with_agent_api(Arc::new(AgentApi {
        managed: Arc::new(adx_agent_api::managed::ManagedService::new(control)),
        request_timeout: adx_agent_core::limits::AGENT_REQUEST_TIMEOUT,
    }));
    // Even a UUID present in ADX state is a literal Sandbox target on shared routes.
    for target in [&id, &format!("adx-{id}")] {
        for connect in [false, true] {
            let uri = if connect {
                format!("{target}:22")
            } else {
                format!("/{target}/8080/")
            };
            let mut request = Request::builder()
                .method(if connect { "CONNECT" } else { "GET" })
                .uri(&uri)
                .header("authorization", "Bearer tenant")
                .body(())
                .unwrap();
            gateway
                .prepare_agent_data(&mut request, IngressSecurity::Tls)
                .await
                .unwrap();
            assert_eq!(request.uri().to_string(), uri);
            assert!(request
                .extensions()
                .get::<AuthenticatedIdentity>()
                .is_none());
        }
    }
    assert_eq!(verifier.0.load(Ordering::SeqCst), 0);
    let mut request = Request::builder()
        .uri("/agent/http/?target=urn:adx:environment:test:1:env&port=8080")
        .header("authorization", "Bearer tenant")
        .body(())
        .unwrap();
    gateway
        .prepare_agent_data(&mut request, IngressSecurity::Tls)
        .await
        .unwrap();
    assert_eq!(
        gateway
            .authenticator
            .authenticate_request_with_policy(&request, true)
            .await
            .unwrap(),
        "tenant"
    );
    assert_eq!(request.uri().path(), format!("/{id}/8080/"));
    assert_eq!(verifier.0.load(Ordering::SeqCst), 1);
    let mut wrong = Request::builder()
        .uri("/agent/http/?target=urn:adx:environment:test:1:env&port=8080")
        .header("authorization", "Bearer other")
        .body(())
        .unwrap();
    let error = gateway
        .prepare_agent_data(&mut wrong, IngressSecurity::Tls)
        .await
        .unwrap_err();
    assert_eq!(error.status(), StatusCode::NOT_FOUND);
}

struct StalledSandbox;
#[async_trait::async_trait]
impl Sandbox for StalledSandbox {
    async fn create(&self, _: &CreateSandbox) -> Result<SandboxObservation, SandboxError> {
        std::future::pending().await
    }
    async fn get(&self, _: &str, _: &str) -> Result<Option<SandboxObservation>, SandboxError> {
        std::future::pending().await
    }
    async fn delete(&self, _: &str, _: &str) -> Result<SandboxObservation, SandboxError> {
        std::future::pending().await
    }
}
#[tokio::test(start_paused = true)]
async fn agent_management_timeout_distinguishes_queries_from_writes() {
    let (gateway, _, _) = fixture_with_backend(Arc::new(StalledSandbox)).await;
    let id = uuid::Uuid::new_v4();
    for (method, path, expected) in [
        ("GET", format!("/api/agent/{id}"), "unavailable"),
        ("POST", "/api/agent".to_owned(), "outcome_unknown"),
        ("DELETE", format!("/api/agent/{id}"), "outcome_unknown"),
    ] {
        let (mut sender, server, client) = connection(gateway.clone(), IngressSecurity::Tls).await;
        let response = sender
            .send_request(request(method, &path, "tenant", "deadline-test", input()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json(response).await["error"]["kind"], expected);
        server.abort();
        client.abort();
    }
}
#[tokio::test]
async fn inline_only_create_and_kill() {
    let config = serde_json::from_value(serde_json::json!({
        "iam_address":iam_server().await, "backend_timeout_seconds":2, "max_inflight":4,
        "inline_profiles":[{"sandbox_type":"docker","request_image":"app:1","image":"app:1","isolation_runtime":"runc","working_dir":"/"}]
    })).unwrap();
    let api = InlineApi::new(config, Arc::new(Backend::default())).unwrap();
    let created = api
        .service
        .create(
            &adx_agent_api::request::RequestContext::new(std::time::Duration::from_secs(60)),
            "tenant",
            serde_json::from_value(input()).unwrap(),
        )
        .await
        .unwrap();
    api.service
        .kill("tenant", &created.instance_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn inline_accepts_frontend_credentials_independently_of_managed_api_keys() {
    let (gateway, _) = fixture().await;
    let control = Arc::new(
        remote_control(adx_activator::Activator::new(
            AgentState::new(Arc::new(MemoryRepository::default())),
            Arc::new(Backend::default()),
        ))
        .await,
    );
    let gateway = Arc::new(
        Arc::try_unwrap(gateway)
            .ok()
            .unwrap()
            .with_agent_api(Arc::new(AgentApi {
                managed: Arc::new(adx_agent_api::managed::ManagedService::new(control)),
                request_timeout: adx_agent_core::limits::AGENT_REQUEST_TIMEOUT,
            })),
    );
    let (mut sender, server, client) = connection(gateway, IngressSecurity::Tls).await;
    let token = inline_token("tenant", "developer", "signature");
    let mut req = request("POST", "/api/agent", "bad", "inline-jwt", input());
    req.headers_mut().insert("x-auth", token.parse().unwrap());
    let response = sender.send_request(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(json(response).await["instance_id"].is_string());
    let template = serde_json::json!({"name":"demo","version":"1","image":"app:1","isolation_runtime":"runc","entrypoint":["/start"],"resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080}]});
    let response = sender
        .send_request(request(
            "POST",
            "/api/agent/v2/templates",
            "tenant",
            "managed-key",
            template.clone(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let published = json(response).await;
    assert_eq!(published["template"]["name"], "demo");
    assert_eq!(published["template"]["version"], "1");
    let response = sender
        .send_request(request(
            "POST",
            "/api/agent/v2/templates",
            &token,
            "wrong-family",
            template,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    response.collect().await.unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/api/agent")
        .header("authorization", "Bearer tenant")
        .body(Full::new(Bytes::from(input().to_string())))
        .unwrap();
    let response = sender.send_request(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(json(response).await["error"].is_string());
    client.abort();
    server.abort();
}

fn inline_token(tenant: &str, role: &str, signature: &str) -> String {
    use base64::Engine;
    let claims = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::json!({"sub":tenant,"role":role,"exp":0}).to_string());
    format!("e30.{claims}.{signature}")
}
async fn iam_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(
                        TokioIo::new(stream),
                        service_fn(|request: Request<Incoming>| async move {
                            assert_eq!(request.method(), http::Method::GET);
                            assert_eq!(request.uri().path(), "/iam-server/v1/token/auth");
                            let token = request.headers()["x-auth"].to_str().unwrap();
                            let status = if ["tenant", "other"].iter().any(|tenant| {
                                token == inline_token(tenant, "developer", "signature")
                            }) {
                                StatusCode::OK
                            } else {
                                StatusCode::UNAUTHORIZED
                            };
                            Ok::<_, std::convert::Infallible>(
                                Response::builder()
                                    .status(status)
                                    .body(Full::new(Bytes::new()))
                                    .unwrap(),
                            )
                        }),
                    )
                    .await;
            });
        }
    });
    address
}

#[tokio::test]
async fn inline_legacy_carriers_preserve_precedence_and_require_verified_developer() {
    let (gateway, _) = fixture().await;
    let (mut sender, server, client) = connection(gateway, IngressSecurity::Tls).await;
    let valid = inline_token("tenant", "developer", "signature");
    let tampered = inline_token("tenant", "developer", "tampered");
    let user = inline_token("tenant", "user", "signature");
    for (path, headers, status) in [
        (
            format!("/api/agent?token={valid}&tenant_id=other"),
            vec![],
            StatusCode::OK,
        ),
        (
            "/api/agent".into(),
            vec![("cookie", format!("iam_token={valid}"))],
            StatusCode::OK,
        ),
        (
            format!("/api/agent?token={tampered}"),
            vec![("x-auth", valid.clone())],
            StatusCode::OK,
        ),
        (
            format!("/api/agent?token={valid}"),
            vec![("x-auth", tampered)],
            StatusCode::UNAUTHORIZED,
        ),
        (
            "/api/agent".into(),
            vec![("x-auth", user)],
            StatusCode::UNAUTHORIZED,
        ),
    ] {
        let mut req = Request::builder().method("POST").uri(path);
        for (name, value) in headers {
            req = req.header(name, value);
        }
        let response = sender
            .send_request(
                req.body(Full::new(Bytes::from(input().to_string())))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        let result = json(response).await;
        if status == StatusCode::OK {
            let id = result["instance_id"].as_str().unwrap();
            let response = sender
                .send_request(request(
                    "GET",
                    &format!("/api/agent/{id}"),
                    "tenant",
                    "verified-subject",
                    serde_json::Value::Null,
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            response.collect().await.unwrap();
        }
    }
    client.abort();
    server.abort();
}

#[tokio::test]
async fn common_http_entry_selects_inline_or_environment_without_auth_fallback() {
    let (gateway, _) = fixture().await;
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let template = serde_json::from_value(serde_json::json!({"name":"demo","version":"1","image":"app:1","isolation_runtime":"runc","entrypoint":["/start"],"resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080},{"protocol":"ws","port":8080}]})).unwrap();
    state.publish("tenant", &template).await.unwrap();
    let api = Arc::new(AgentApi {
        managed: Arc::new(adx_agent_api::managed::ManagedService::new(Arc::new(
            remote_control(adx_activator::Activator::new(
                state.clone(),
                Arc::new(Backend::default()),
            ))
            .await,
        ))),
        request_timeout: adx_agent_core::limits::AGENT_REQUEST_TIMEOUT,
    });
    let gateway = Arc::try_unwrap(gateway).ok().unwrap().with_agent_api(api);
    let mut inline = Request::builder()
        .uri("/agent/http/chat?instance=direct-id&q=one&q=two")
        .header("x-auth", inline_token("tenant", "developer", "signature"))
        .body(())
        .unwrap();
    gateway
        .prepare_agent_data(&mut inline, IngressSecurity::Tls)
        .await
        .unwrap();
    assert_eq!(
        inline.uri().to_string(),
        "/direct-id/18092/chat?q=one&q=two"
    );
    let mut managed = Request::builder()
        .uri("/agent/http/chat?target=urn:adx:template:demo:1&q=one")
        .header("authorization", "Bearer tenant")
        .body(())
        .unwrap();
    gateway
        .prepare_agent_data(&mut managed, IngressSecurity::Tls)
        .await
        .unwrap();
    let listing = adx_agent_core::activator::EnvironmentList {
        tenant: "tenant".into(),
        template: "demo".into(),
        version: "1".into(),
        page_size: 10,
        page_token: None,
    };
    let page = state.list_environments(&listing).await.unwrap();
    assert_eq!(page.environments.len(), 1);
    assert_eq!(
        managed.uri().to_string(),
        format!("/{}/8080/chat?q=one", page.environments[0].sandbox_id)
    );
    for (path, token, status) in [
        (
            "/agent/http?instance=x&target=urn:adx:template:demo:1",
            "tenant",
            StatusCode::BAD_REQUEST,
        ),
        ("/agent/http?instance=x", "tenant", StatusCode::UNAUTHORIZED),
        (
            "/agent/http?target=urn:adx:template:demo:1",
            "wrong",
            StatusCode::UNAUTHORIZED,
        ),
    ] {
        let mut req = Request::builder()
            .uri(path)
            .header("authorization", format!("Bearer {token}"))
            .body(())
            .unwrap();
        assert_eq!(
            gateway
                .prepare_agent_data(&mut req, IngressSecurity::Tls)
                .await
                .unwrap_err()
                .status(),
            status
        );
    }
    let (mut sender, server, client) = connection(Arc::new(gateway), IngressSecurity::Tls).await;
    let response = sender
        .send_request(
            Request::builder()
                .uri("/agent/http?target=urn:adx:template:demo:1")
                .header("authorization", "Bearer tenant")
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND); // The test Platform has not published a route.
    let id = response.headers()["x-adx-environment-id"]
        .to_str()
        .unwrap()
        .to_owned();
    uuid::Uuid::parse_str(&id).unwrap();
    assert_eq!(
        response.headers()["x-adx-environment-urn"],
        format!("urn:adx:environment:demo:1:{id}")
    );
    response.collect().await.unwrap();
    let page = state.list_environments(&listing).await.unwrap();
    assert_eq!(page.environments.len(), 2); // One new request, even though forwarding retries activation.
    assert!(page
        .environments
        .iter()
        .any(|env| env.scope.environment_id == id));
    client.abort();
    server.abort();
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn unified_access_forwards_payloads_and_returns_environment_headers() {
    use crate::{common::protocol::GatewayPolicy, node::Relay};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (seen_tx, mut seen) = mpsc::unbounded_channel();
    let backend_task = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let seen_tx = seen_tx.clone();
            tokio::spawn(async move {
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(
                        TokioIo::new(stream),
                        service_fn(move |mut req: Request<Incoming>| {
                            let seen_tx = seen_tx.clone();
                            async move {
                                let uri = req.uri().to_string();
                                let headers = req.headers().clone();
                                let response = if req.headers().contains_key("upgrade") {
                                    let upgrade = hyper::upgrade::on(&mut req);
                                    tokio::spawn(async move {
                                        let mut stream = TokioIo::new(upgrade.await.unwrap());
                                        let mut bytes = [0; 5];
                                        stream.read_exact(&mut bytes).await.unwrap();
                                        stream.write_all(&bytes).await.unwrap();
                                    });
                                    Response::builder()
                                        .status(101)
                                        .header("connection", "upgrade")
                                        .header("upgrade", "websocket")
                                        .header("sec-websocket-accept", "test-accept")
                                        .body(Full::new(Bytes::new()))
                                        .unwrap()
                                } else {
                                    let body = req.into_body().collect().await.unwrap().to_bytes();
                                    assert_eq!(body, Bytes::from_static(b"business-body"));
                                    Response::builder()
                                        .status(201)
                                        .header("x-adx-environment-id", "backend-spoof")
                                        .header("x-adx-environment-urn", "backend-spoof")
                                        .header("x-business", "kept")
                                        .body(Full::new(Bytes::from_static(b"business-result")))
                                        .unwrap()
                                };
                                seen_tx.send((uri, headers)).unwrap();
                                Ok::<_, std::convert::Infallible>(response)
                            }
                        }),
                    )
                    .with_upgrades()
                    .await;
            });
        }
    });
    let node = Arc::new(
        Relay::new(GatewayPolicy::for_local_mock(vec!["127.0.0.0/8"
            .parse()
            .unwrap()]))
        .with_route_enforcement(),
    );
    let node_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_address = node_listener.local_addr().unwrap();
    let node_task = {
        let node = node.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = node_listener.accept().await.unwrap();
                let node = node.clone();
                tokio::spawn(async move {
                    let _ = node.serve_h2(stream).await;
                });
            }
        })
    };
    let (gateway, _, routes) = fixture_with_routes().await;
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let template = serde_json::from_value(serde_json::json!({"name":"demo","version":"1","image":"app:1","isolation_runtime":"runc","entrypoint":["/start"],"resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":port},{"protocol":"ws","port":port}]})).unwrap();
    state.publish("tenant", &template).await.unwrap();
    let scope = adx_agent_core::Scope {
        tenant: "tenant".into(),
        template: "demo".into(),
        version: "1".into(),
        environment_id: "env".into(),
    };
    let environment = state.create_environment(scope).await.unwrap();
    for id in ["api", &environment.sandbox_id] {
        let route: crate::common::route::RouteInfo = serde_json::from_value(serde_json::json!({"instanceID":id,"instanceStatus":{"code":3},"tenantID":"tenant","sandboxID":format!("runtime-{id}"),"sandboxIP":"127.0.0.1","nodeProxyAddress":node_address.to_string()})).unwrap();
        node.activate_route(
            route.instance_id.clone(),
            route.sandbox_id.clone(),
            route.sandbox_ip.parse().unwrap(),
        )
        .await;
        routes.put(route);
    }
    let api = Arc::new(AgentApi {
        managed: Arc::new(adx_agent_api::managed::ManagedService::new(Arc::new(
            remote_control(adx_activator::Activator::new(
                state,
                Arc::new(Backend::default()),
            ))
            .await,
        ))),
        request_timeout: adx_agent_core::limits::AGENT_REQUEST_TIMEOUT,
    });
    let mut gateway = Arc::try_unwrap(gateway).ok().unwrap().with_agent_api(api);
    gateway.control_plane_routes = Arc::new(vec![StaticRoute::Prefix("/api".into())]);
    let gateway = Arc::new(gateway);
    let token = inline_token("tenant", "developer", "signature");
    for inline in [true, false] {
        let (mut sender, server, client) = connection(gateway.clone(), IngressSecurity::Tls).await;
        let selector = if inline {
            "instance=api"
        } else {
            "target=urn:adx:environment:demo:1:env"
        };
        let req = Request::builder()
            .method("POST")
            .uri(format!("/agent/http/chat?{selector}&port={port}&q=a&q=b"))
            .header("host", "public.example")
            .header("x-business", "keep")
            .header(
                if inline { "x-auth" } else { "authorization" },
                if inline {
                    token.as_str()
                } else {
                    "Bearer tenant"
                },
            )
            .body(Full::new(Bytes::from_static(b"business-body")))
            .unwrap();
        let response = sender.send_request(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers()["x-business"], "kept");
        if !inline {
            assert_eq!(response.headers()["x-adx-environment-id"], "env");
            assert_eq!(
                response.headers()["x-adx-environment-urn"],
                "urn:adx:environment:demo:1:env"
            );
        }
        assert_eq!(
            response.collect().await.unwrap().to_bytes(),
            Bytes::from_static(b"business-result")
        );
        let (uri, headers) = seen.recv().await.unwrap();
        assert_eq!(uri, "/chat?q=a&q=b");
        assert_eq!(headers["host"], "public.example");
        assert_eq!(headers["x-business"], "keep");
        if inline {
            assert_eq!(headers["x-auth"], token);
            assert_eq!(headers["x-forwarded-proto"], "https");
        }
        client.abort();
        server.abort();

        let (mut sender, server, client) = connection(gateway.clone(), IngressSecurity::Tls).await;
        let mut req = Request::builder()
            .uri(format!("/agent/ws?{selector}&port={port}"))
            .header("host", "public.example")
            .header("connection", "upgrade")
            .header("upgrade", "websocket")
            .header(
                "sec-websocket-protocol",
                if inline { token.as_str() } else { "chat" },
            );
        if !inline {
            req = req.header("authorization", "Bearer tenant");
        }
        let mut response = sender
            .send_request(req.body(Full::new(Bytes::new())).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        if !inline {
            assert_eq!(response.headers()["x-adx-environment-id"], "env");
        }
        let mut upgraded = TokioIo::new(hyper::upgrade::on(&mut response).await.unwrap());
        upgraded.write_all(b"\x82\x03abc").await.unwrap();
        let mut echoed = [0; 5];
        upgraded.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"\x82\x03abc");
        let (uri, headers) = seen.recv().await.unwrap();
        assert_eq!(
            uri,
            if inline {
                format!("/serverless/v1/ws?{selector}&port={port}")
            } else {
                "/".into()
            }
        );
        assert_eq!(
            headers["sec-websocket-protocol"],
            if inline { token.as_str() } else { "chat" }
        );
        client.abort();
        server.abort();
    }
    backend_task.abort();
    node_task.abort();
}

#[cfg(feature = "mock-e2e")]
#[path = "ssh/terminal_tests.rs"]
mod ssh_terminal;

#[tokio::test]
async fn managed_http_and_ws_activation_are_bounded_by_the_entry_deadline() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let template = serde_json::from_value(serde_json::json!({"name":"demo","version":"1","image":"app:1","isolation_runtime":"runc","entrypoint":["/start"],"resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080},{"protocol":"ws","port":8080}]})).unwrap();
    state.publish("tenant", &template).await.unwrap();
    let api = AgentApi {
        managed: Arc::new(adx_agent_api::managed::ManagedService::new(Arc::new(
            remote_control(adx_activator::Activator::new(
                state,
                Arc::new(StalledSandbox),
            ))
            .await,
        ))),
        request_timeout: std::time::Duration::from_secs(1),
    };
    for protocol in ["http", "ws"] {
        let mut builder = Request::builder().uri(format!(
            "/agent/{protocol}?target=urn:adx:environment:demo:1:env"
        ));
        if protocol == "ws" {
            builder = builder.header("upgrade", "websocket");
        }
        let mut request = builder.body(()).unwrap();
        let access = crate::ingress::agent_access::AccessRequest::parse(&request)
            .unwrap()
            .unwrap();
        let started = tokio::time::Instant::now();
        assert!(matches!(
            api.prepare_data(&mut request, "tenant", access).await,
            Err(adx_agent_api::Error::OutcomeUnknown(_))
        ));
        assert!(
            (std::time::Duration::from_millis(900)..std::time::Duration::from_secs(3))
                .contains(&started.elapsed())
        );
    }
}

#[tokio::test]
async fn managed_target_retry_uses_the_original_deadline() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let template = serde_json::from_value(serde_json::json!({"name":"demo","version":"1","image":"app:1","isolation_runtime":"runc","entrypoint":["/start"],"resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080}]})).unwrap();
    state.publish("tenant", &template).await.unwrap();
    let api = AgentApi {
        managed: Arc::new(adx_agent_api::managed::ManagedService::new(Arc::new(
            remote_control(adx_activator::Activator::new(
                state,
                Arc::new(Backend::default()),
            ))
            .await,
        ))),
        request_timeout: std::time::Duration::from_secs(1),
    };
    let mut request = Request::builder()
        .uri("/agent/http?target=urn:adx:environment:demo:1:env")
        .body(())
        .unwrap();
    let access = crate::ingress::agent_access::AccessRequest::parse(&request)
        .unwrap()
        .unwrap();
    api.prepare_data(&mut request, "tenant", access)
        .await
        .unwrap();
    let selected = request.uri().clone();
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert!(matches!(
        api.retry_data(&mut request).await,
        Err(adx_agent_api::Error::Unavailable(_))
    ));
    assert_eq!(request.uri(), &selected);
}

#[cfg(feature = "mock-e2e")]
struct RuntimeBackend {
    inner: Backend,
    port: u16,
}
#[cfg(feature = "mock-e2e")]
#[async_trait::async_trait]
impl Sandbox for RuntimeBackend {
    async fn create(&self, request: &CreateSandbox) -> Result<SandboxObservation, SandboxError> {
        self.inner.create(request).await
    }
    async fn get(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<SandboxObservation>, SandboxError> {
        self.inner.get(tenant, id).await
    }
    async fn delete(&self, tenant: &str, id: &str) -> Result<SandboxObservation, SandboxError> {
        self.inner.delete(tenant, id).await
    }
    async fn list(&self, tenant: &str) -> Result<Vec<SandboxInfo>, SandboxError> {
        self.inner.list(tenant).await
    }
    async fn runtime(&self, tenant: &str, id: &str) -> Result<SandboxRuntime, SandboxError> {
        self.inner
            .get(tenant, id)
            .await?
            .ok_or(SandboxError::NotFound)?;
        Ok(SandboxRuntime {
            port: self.port,
            token: "runtime-only-secret".into(),
        })
    }
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn inline_runtime_operations_use_authorized_node_tunnel_and_original_wire_contract() {
    use crate::{common::protocol::GatewayPolicy, node::Relay};
    use serde_json::json as value;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let payload = Arc::new(Mutex::new(Vec::new()));
    let uploaded = payload.clone();
    let runtime = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let uploaded = uploaded.clone();
            tokio::spawn(async move {
                let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), service_fn(move |request: Request<Incoming>| {
                    let uploaded = uploaded.clone();
                    async move {
                        assert_eq!(request.headers()["x-auth"], "runtime-only-secret");
                        assert_eq!(request.headers()["x-trace-id"], "operation-trace");
                        let uri = request.uri().clone();
                        if uri.path() == "/download" {
                            assert_eq!(request.headers()["range"], "bytes=1-3");
                            return Ok::<_, Infallible>(Response::builder().status(206).header("content-range","bytes 1-3/5").body(Full::new(Bytes::from_static(b"ell"))).unwrap());
                        }
                        let body = request.into_body().collect().await.unwrap().to_bytes();
                        let result = match uri.path() {
                            "/invoke" => {
                                let input: serde_json::Value = serde_json::from_slice(&body).unwrap();
                                match input["action"].as_str().unwrap() {
                                    "cmd_run" => { assert_eq!(input["args"]["cmd"], "exec 'printf' 'hello'"); value!({"exit_code":0,"stdout":"hello","stderr":""}) },
                                    "fs_list" => { assert_eq!(input["args"]["depth"],2); value!({"error":null,"entries":[{"name":"f","path":"/tmp/f","type":"file","size":5,"modified_time":0.0}]}) },
                                    "fs_get_info" => value!({"size":5,"error":null}),
                                    "fs_make_dir" => value!({"created":true,"error":null}),
                                    other => panic!("unexpected action {other}"),
                                }
                            }
                            "/upload" => { uploaded.lock().unwrap().extend_from_slice(&body); value!({"error":null,"committed":false,"bytes_written":body.len()}) },
                            "/upload/commit" => { assert_eq!(*uploaded.lock().unwrap(), b"hello"); value!({"error":null,"committed":true,"size":5}) },
                            other => panic!("unexpected runtime path {other}"),
                        };
                        Ok(Response::new(Full::new(Bytes::from(result.to_string()))))
                    }
                })).await;
            });
        }
    });
    let node = Arc::new(
        Relay::new(GatewayPolicy::for_local_mock(vec!["127.0.0.0/8"
            .parse()
            .unwrap()]))
        .with_route_enforcement(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let node_task = {
        let node = node.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let node = node.clone();
                tokio::spawn(async move {
                    let _ = node.serve_h2(stream).await;
                });
            }
        })
    };
    let (gateway, api, routes) = fixture_with_backend(Arc::new(RuntimeBackend {
        inner: Backend::default(),
        port,
    }))
    .await;
    let id = api
        .service
        .create(
            &adx_agent_api::request::RequestContext::new(std::time::Duration::from_secs(60)),
            "tenant",
            serde_json::from_value(input()).unwrap(),
        )
        .await
        .unwrap()
        .instance_id;
    let route: crate::common::route::RouteInfo = serde_json::from_value(value!({"instanceID":id,"instanceStatus":{"code":3},"tenantID":"tenant","sandboxID":"runtime","sandboxIP":"127.0.0.1","nodeProxyAddress":address.to_string()})).unwrap();
    node.activate_route(
        route.instance_id.clone(),
        route.sandbox_id.clone(),
        route.sandbox_ip.parse().unwrap(),
    )
    .await;
    routes.put(route);
    let (mut sender, server, client) = connection(gateway, IngressSecurity::Tls).await;
    let listed = sender
        .send_request(request("GET", "/api/agent", "tenant", "list", value!(null)))
        .await
        .unwrap();
    assert_eq!(json(listed).await["instances"][0]["instance_id"], id);
    let other = sender
        .send_request(request("GET", "/api/agent", "other", "list", value!(null)))
        .await
        .unwrap();
    assert_eq!(json(other).await["instances"], value!([]));
    for (method, suffix, body, expected) in [
        (
            "POST",
            "exec",
            value!({"command":["printf","hello"],"timeout":2}),
            value!({"returncode":0,"stdout":"hello","stderr":""}),
        ),
        (
            "POST",
            "files/mkdir?path=/tmp/new&recursive=true",
            value!(null),
            value!({"success":true,"path":"/tmp/new","created":true}),
        ),
    ] {
        let response = sender
            .send_request(request(
                method,
                &format!("/api/agent/{id}/{suffix}"),
                "tenant",
                "operation-trace",
                body,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(json(response).await, expected);
    }
    let response = sender
        .send_request(request(
            "GET",
            &format!("/api/agent/{id}/files/list?path=/tmp&recursive=true&max_depth=2"),
            "tenant",
            "operation-trace",
            value!(null),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json(response).await["items"][0]["path"], "/tmp/f");
    let mut download = request(
        "GET",
        &format!("/api/agent/{id}/files/download?path=/tmp/f"),
        "tenant",
        "operation-trace",
        value!(null),
    );
    download
        .headers_mut()
        .insert("range", "bytes=1-3".parse().unwrap());
    let response = sender.send_request(download).await.unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers()["content-range"], "bytes 1-3/5");
    assert_eq!(response.collect().await.unwrap().to_bytes(), "ell");
    let mut upload = request(
        "POST",
        &format!("/api/agent/{id}/files/upload"),
        "tenant",
        "operation-trace",
        value!(null),
    );
    upload.headers_mut().insert(
        "content-type",
        "multipart/form-data; boundary=BOUND".parse().unwrap(),
    );
    *upload.body_mut() = Full::new(Bytes::from_static(b"--BOUND\r\nContent-Disposition: form-data; name=\"path\"\r\n\r\n/tmp/f\r\n--BOUND\r\nContent-Disposition: form-data; name=\"file\"; filename=\"f\"\r\n\r\nhello\r\n--BOUND--\r\n"));
    let response = sender.send_request(upload).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        json(response).await,
        value!({"success":true,"path":"/tmp/f","size":5})
    );
    let denied = sender
        .send_request(request(
            "POST",
            &format!("/api/agent/{id}/exec"),
            "other",
            "operation-trace",
            value!({"command":"echo secret"}),
        ))
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::NOT_FOUND);
    json(denied).await;
    server.abort();
    client.abort();
    node_task.abort();
    runtime.abort();
}
