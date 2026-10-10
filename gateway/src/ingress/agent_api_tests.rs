//! Transport and fixed-ID mapping checks; no live Platform claim.
use super::*;
use crate::ingress::{
    agent_api::AgentApi,
    auth::{AuthError, AuthenticatedIdentity, CredentialVerifier},
    H2PoolConfig, RouteStore,
};
use adx_agent_core::sandbox::*;
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
async fn fixture() -> (Arc<Ingress>, Arc<dyn Sandbox>) {
    let (gateway, backend, _) = fixture_with_routes().await;
    (gateway, backend)
}
async fn fixture_with_routes() -> (Arc<Ingress>, Arc<dyn Sandbox>, Arc<RouteStore>) {
    fixture_with_backend(Arc::new(Backend::default())).await
}
async fn fixture_with_backend(
    backend: Arc<dyn Sandbox>,
) -> (Arc<Ingress>, Arc<dyn Sandbox>, Arc<RouteStore>) {
    let store = Arc::new(RouteStore::new());
    store.set_ready(true);
    let gateway = Arc::new(Ingress::new(
        Arc::new(IngressRouteResolver::new(store.clone())),
        DataPlaneL4Connector::new(H2PoolConfig::default()),
        IngressAuthenticator::with_verifier(Arc::new(Identity)),
        50090,
        8765,
        "127.0.0.1:1",
        vec![],
    ));
    (gateway, backend, store)
}
async fn create_sandbox(backend: &dyn Sandbox) -> String {
    let request: CreateSandbox = serde_json::from_value(serde_json::json!({
        "id":uuid::Uuid::new_v4().to_string(),"tenant":"tenant","execution":{
            "image":"app:1","isolation_runtime":"runc","inherit_entrypoint":true,
            "entrypoint":[],"working_dir":"","user":null,"env":{},
            "resources":{"cpu_millis":1000,"memory_mib":512},"service":[]
        }
    }))
    .unwrap();
    backend.create(&request).await.unwrap().id
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
    let builder = Request::builder()
        .method(method)
        .uri(path)
        .header("x-trace-id", trace)
        .header("authorization", format!("Bearer {token}"));
    builder
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap()
}
async fn json(response: Response<Incoming>) -> serde_json::Value {
    serde_json::from_slice(&response.collect().await.unwrap().to_bytes()).unwrap()
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
    let template = serde_json::from_value(serde_json::json!({"name":"demo","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080}]})).unwrap();
    a.managed
        .publish(&control_context(), "tenant", &template)
        .await
        .unwrap();
    let scope = adx_agent_core::Scope {
        tenant: "tenant".into(),
        template: "demo".into(),
        version: "1".into(),
        binding_id: "env".into(),
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
        .delete_binding(&control_context(), &scope)
        .await
        .unwrap();
    let fresh = a
        .managed
        .resolve(&ctx, &scope, adx_agent_core::Protocol::Http, None)
        .await
        .unwrap();
    assert_ne!(original.0.binding.generation, fresh.0.binding.generation);
    assert_ne!(original.0.binding.sandbox_id, fresh.0.binding.sandbox_id);
    a.managed
        .delete_binding(&control_context(), &scope)
        .await
        .unwrap();
}
#[tokio::test]
async fn managed_binding_management_and_protocol_forwarding() {
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
    let template = serde_json::json!({"name":"demo","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080},{"protocol":"ws","port":8080},{"protocol":"ssh","port":22}]});
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
    let path = "/api/agent/v2/templates/demo/versions/1/bindings/env";
    let mut traffic = Request::builder()
        .uri("/agent/http/path?target=urn:adx:binding:demo:1:env&port=8080")
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
    let binding = json(response).await["binding"].clone();
    let listing = "/api/agent/v2/templates/demo/versions/1/bindings";
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
    assert_eq!(page["bindings"], serde_json::json!([binding.clone()]));
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
    let id = binding["sandbox_id"].as_str().unwrap();
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
                "/agent/{protocol}/path?target=urn:adx:binding:demo:1:env&port=8080&q=1"
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
        .uri("/agent/http/path?target=urn:adx:binding:demo:1:env&port=8080")
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
    // A real Sandbox ID must not shadow a literal Platform route.
    let id = create_sandbox(api.as_ref()).await;
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
        binding_id: "env".into(),
    };
    let template = serde_json::from_value(serde_json::json!({"name":"test","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080}]})).unwrap();
    state.publish("tenant", &template).await.unwrap();
    state.create_binding(scope.clone()).await.unwrap();
    let id = state.binding(&scope).await.unwrap().unwrap().sandbox_id;
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
        .uri("/agent/http/?target=urn:adx:binding:test:1:env&port=8080")
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
        .uri("/agent/http/?target=urn:adx:binding:test:1:env&port=8080")
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
#[tokio::test]
async fn managed_http_entry_authenticates_before_creating_binding() {
    let (gateway, _) = fixture().await;
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let template = serde_json::from_value(serde_json::json!({"name":"demo","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080},{"protocol":"ws","port":8080}]})).unwrap();
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
    let mut managed = Request::builder()
        .uri("/agent/http/chat?target=urn:adx:template:demo:1&q=one")
        .header("authorization", "Bearer tenant")
        .body(())
        .unwrap();
    gateway
        .prepare_agent_data(&mut managed, IngressSecurity::Tls)
        .await
        .unwrap();
    let listing = adx_agent_core::activator::BindingList {
        tenant: "tenant".into(),
        template: "demo".into(),
        version: "1".into(),
        page_size: 10,
        page_token: None,
    };
    let page = state.list_bindings(&listing).await.unwrap();
    assert_eq!(page.bindings.len(), 1);
    assert_eq!(
        managed.uri().to_string(),
        format!("/{}/8080/chat?q=one", page.bindings[0].sandbox_id)
    );
    for (path, token, status) in [
        (
            "/agent/http?instance=x&target=urn:adx:template:demo:1",
            "tenant",
            StatusCode::BAD_REQUEST,
        ),
        ("/agent/http?instance=x", "tenant", StatusCode::BAD_REQUEST),
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
    let id = response.headers()["x-adx-binding-id"]
        .to_str()
        .unwrap()
        .to_owned();
    uuid::Uuid::parse_str(&id).unwrap();
    assert_eq!(
        response.headers()["x-adx-binding-urn"],
        format!("urn:adx:binding:demo:1:{id}")
    );
    response.collect().await.unwrap();
    let page = state.list_bindings(&listing).await.unwrap();
    assert_eq!(page.bindings.len(), 2); // One new request, even though forwarding retries activation.
    assert!(page.bindings.iter().any(|env| env.scope.binding_id == id));
    client.abort();
    server.abort();
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn unified_access_forwards_payloads_and_returns_binding_headers() {
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
                                        .header("x-adx-binding-id", "backend-spoof")
                                        .header("x-adx-binding-urn", "backend-spoof")
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
    let template = serde_json::from_value(serde_json::json!({"name":"demo","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":port},{"protocol":"ws","port":port}]})).unwrap();
    state.publish("tenant", &template).await.unwrap();
    let scope = adx_agent_core::Scope {
        tenant: "tenant".into(),
        template: "demo".into(),
        version: "1".into(),
        binding_id: "env".into(),
    };
    let binding = state.create_binding(scope).await.unwrap();
    for id in ["api", &binding.sandbox_id] {
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
    {
        let (mut sender, server, client) = connection(gateway.clone(), IngressSecurity::Tls).await;
        let selector = "target=urn:adx:binding:demo:1:env";
        let req = Request::builder()
            .method("POST")
            .uri(format!("/agent/http/chat?{selector}&port={port}&q=a&q=b"))
            .header("host", "public.example")
            .header("x-business", "keep")
            .header("authorization", "Bearer tenant")
            .body(Full::new(Bytes::from_static(b"business-body")))
            .unwrap();
        let response = sender.send_request(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers()["x-business"], "kept");
        {
            assert_eq!(response.headers()["x-adx-binding-id"], "env");
            assert_eq!(
                response.headers()["x-adx-binding-urn"],
                "urn:adx:binding:demo:1:env"
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
        client.abort();
        server.abort();

        let (mut sender, server, client) = connection(gateway.clone(), IngressSecurity::Tls).await;
        let mut req = Request::builder()
            .uri(format!("/agent/ws?{selector}&port={port}"))
            .header("host", "public.example")
            .header("connection", "upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-protocol", "chat");
        {
            req = req.header("authorization", "Bearer tenant");
        }
        let mut response = sender
            .send_request(req.body(Full::new(Bytes::new())).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        {
            assert_eq!(response.headers()["x-adx-binding-id"], "env");
        }
        let mut upgraded = TokioIo::new(hyper::upgrade::on(&mut response).await.unwrap());
        upgraded.write_all(b"\x82\x03abc").await.unwrap();
        let mut echoed = [0; 5];
        upgraded.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"\x82\x03abc");
        let (uri, headers) = seen.recv().await.unwrap();
        assert_eq!(uri, "/");
        assert_eq!(headers["sec-websocket-protocol"], "chat");
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
    let template = serde_json::from_value(serde_json::json!({"name":"demo","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080},{"protocol":"ws","port":8080}]})).unwrap();
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
            "/agent/{protocol}?target=urn:adx:binding:demo:1:env"
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
    let template = serde_json::from_value(serde_json::json!({"name":"demo","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080}]})).unwrap();
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
        .uri("/agent/http?target=urn:adx:binding:demo:1:env")
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
    queries: std::sync::atomic::AtomicUsize,
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
}

#[cfg(feature = "mock-e2e")]
#[async_trait::async_trait]
impl crate::ingress::sandbox_files::SandboxDirectory for RuntimeBackend {
    async fn authorize(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<(), crate::ingress::sandbox_files::ReadError> {
        use crate::ingress::sandbox_files::ReadError;
        self.queries
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let observed = self
            .inner
            .get(tenant, id)
            .await
            .map_err(|_| ReadError::NotFound)?
            .ok_or(ReadError::NotFound)?;
        if observed.phase != SandboxPhase::Running {
            return Err(ReadError::Runtime);
        }
        Ok(())
    }
}
#[cfg(feature = "mock-e2e")]
fn with_files(gateway: &Ingress, backend: Arc<RuntimeBackend>) -> Ingress {
    let port = backend.port;
    gateway.clone().with_execd_access(Arc::new(
        crate::ingress::sandbox_files::ExecdAccess::new(
            backend,
            port,
            "runtime-only-secret".into(),
        )
        .unwrap(),
    ))
}

#[tokio::test]
async fn shared_service_access_retries_only_unsent_generation_and_checks_tenant() {
    use crate::ingress::agent_service::{AgentV2Access, ServiceAccessError, ServiceSelector};
    use adx_agent_api::request::RequestContext;
    use adx_agent_core::{Protocol, Scope};
    let (gateway, _, routes) = fixture_with_routes().await;
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let template = serde_json::from_value(serde_json::json!({"name":"demo","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"ws","port":8080}]})).unwrap();
    state.publish("tenant", &template).await.unwrap();
    let backend = Arc::new(Backend::default());
    let managed = Arc::new(adx_agent_api::managed::ManagedService::new(Arc::new(
        remote_control(adx_activator::Activator::new(
            state.clone(),
            backend.clone(),
        ))
        .await,
    )));
    let access = AgentV2Access::new(managed.clone());
    let scope = Scope {
        tenant: "tenant".into(),
        template: "demo".into(),
        version: "1".into(),
        binding_id: "binding".into(),
    };
    let selector = ServiceSelector {
        protocol: Protocol::Ws,
        port: None,
    };
    let selection = access
        .select(
            Arc::new(RequestContext::new(std::time::Duration::from_secs(2))),
            scope.clone(),
            selector,
        )
        .await
        .unwrap();
    let binding = state.binding(&scope).await.unwrap().unwrap();
    let before = backend.1.load(Ordering::SeqCst);
    let error = access
        .connect_service(&gateway, selection)
        .await
        .err()
        .unwrap();
    assert!(matches!(
        error,
        ServiceAccessError::Ingress(IngressOpenError::Resolve(ResolveError::NotFound))
    ));
    assert_eq!(
        backend.1.load(Ordering::SeqCst),
        before + 1,
        "one forced observation after unsent failure"
    );

    let selection = access
        .select(
            Arc::new(RequestContext::new(std::time::Duration::from_secs(2))),
            scope.clone(),
            selector,
        )
        .await
        .unwrap();
    routes.put(serde_json::from_value(serde_json::json!({"instanceID":binding.sandbox_id,"instanceStatus":{"code":3},"tenantID":"other","sandboxID":"runtime","sandboxIP":"127.0.0.1","nodeProxyAddress":"127.0.0.1:1"})).unwrap());
    let before = backend.1.load(Ordering::SeqCst);
    assert!(matches!(
        access.connect_service(&gateway, selection).await,
        Err(ServiceAccessError::Ingress(IngressOpenError::Forbidden))
    ));
    assert_eq!(
        backend.1.load(Ordering::SeqCst),
        before,
        "authorization failure must not trigger activation retry"
    );

    let deadline = Arc::new(RequestContext::new(std::time::Duration::from_secs(2)));
    let expired = access
        .select(deadline.clone(), scope.clone(), selector)
        .await
        .unwrap();
    tokio::time::sleep_until(deadline.deadline()).await;
    let before = backend.1.load(Ordering::SeqCst);
    assert!(matches!(
        access.connect_service(&gateway, expired).await,
        Err(ServiceAccessError::Agent(
            adx_agent_api::Error::Unavailable(_)
        ))
    ));
    assert_eq!(
        backend.1.load(Ordering::SeqCst),
        before,
        "expired deadline cannot trigger activation retry"
    );

    let pending = access
        .select(
            Arc::new(RequestContext::new(std::time::Duration::from_secs(2))),
            scope.clone(),
            selector,
        )
        .await
        .unwrap();
    managed
        .delete_binding(
            &RequestContext::new(std::time::Duration::from_secs(2)),
            &scope,
        )
        .await
        .unwrap();
    routes.delete(&binding.sandbox_id);
    assert!(matches!(
        access.connect_service(&gateway, pending).await,
        Err(ServiceAccessError::Agent(adx_agent_api::Error::Conflict(_)))
    ));
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn jiuwen_cold_start_waits_for_backend_listener_before_ws_handshake() {
    use crate::ingress::{agent_service::ServiceSelector, jiuwen::connection::Connection};
    use crate::{common::protocol::GatewayPolicy, node::Relay};
    use adx_agent_api::request::RequestContext;
    use adx_agent_core::{Protocol, Scope};
    use std::time::Duration;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let node = Arc::new(
        Relay::new(GatewayPolicy::for_local_mock(vec!["127.0.0.0/8"
            .parse()
            .unwrap()]))
        .with_route_enforcement(),
    );
    let node_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = node_listener.local_addr().unwrap();
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
    let template = serde_json::from_value(serde_json::json!({"name":"demo","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"ws","port":port}]})).unwrap();
    state.publish("tenant", &template).await.unwrap();
    let scope = Scope {
        tenant: "tenant".into(),
        template: "demo".into(),
        version: "1".into(),
        binding_id: "jiuwen".into(),
    };
    let binding = state.create_binding(scope.clone()).await.unwrap();
    let route: crate::common::route::RouteInfo = serde_json::from_value(serde_json::json!({"instanceID":binding.sandbox_id,"instanceStatus":{"code":3},"tenantID":"tenant","sandboxID":"runtime-jiuwen","sandboxIP":"127.0.0.1","nodeProxyAddress":address.to_string()})).unwrap();
    node.activate_route(
        route.instance_id.clone(),
        route.sandbox_id.clone(),
        route.sandbox_ip.parse().unwrap(),
    )
    .await;
    routes.put(route.clone());
    let reconcile = tokio::spawn(gateway.clone().run_route_reconciler(routes.subscribe()));
    let access = super::super::agent_service::AgentV2Access::new(Arc::new(
        adx_agent_api::managed::ManagedService::new(Arc::new(
            remote_control(adx_activator::Activator::new(
                state,
                Arc::new(Backend::default()),
            ))
            .await,
        )),
    ));
    let selector = ServiceSelector {
        protocol: Protocol::Ws,
        port: None,
    };
    let selection = access
        .select(
            Arc::new(RequestContext::new(Duration::from_secs(3))),
            scope.clone(),
            selector,
        )
        .await
        .unwrap();
    let connecting = {
        let access = access.clone();
        let gateway = gateway.clone();
        tokio::spawn(async move { Connection::connect(&gateway, &access, selection).await })
    };
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(
        !connecting.is_finished(),
        "a refused cold-start port must wait within the admission deadline"
    );
    let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    let (stream, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let backend = tokio_tungstenite::accept_async(stream).await.unwrap();
    let connection = connecting.await.unwrap().unwrap();
    drop(connection);
    drop(backend);
    drop(listener);
    let selection = access
        .select(
            Arc::new(RequestContext::new(Duration::from_millis(150))),
            scope,
            selector,
        )
        .await
        .unwrap();
    let started = tokio::time::Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        Connection::connect(&gateway, &access, selection),
    )
    .await
    .expect("refused port must not outlive the admission deadline");
    assert!(result.is_err());
    assert!(started.elapsed() >= Duration::from_millis(100));
    reconcile.abort();
    node_task.abort();
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn jiuwen_concurrent_connections_obey_handshake_deadline_and_revocation() {
    use crate::ingress::{
        agent_service::ServiceSelector,
        jiuwen::{
            connection::{Connection, ConnectionError},
            driver::DriverConfig,
            session::SessionLimits,
        },
    };
    use crate::{common::protocol::GatewayPolicy, node::Relay};
    use adx_agent_api::request::RequestContext;
    use adx_agent_core::{Protocol, Scope};
    use futures_util::{SinkExt, StreamExt};
    use std::time::Duration;
    use tokio_tungstenite::tungstenite::Message;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let node = Arc::new(
        Relay::new(GatewayPolicy::for_local_mock(vec!["127.0.0.0/8"
            .parse()
            .unwrap()]))
        .with_route_enforcement(),
    );
    let node_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = node_listener.local_addr().unwrap();
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
    let template = serde_json::from_value(serde_json::json!({"name":"demo","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"ws","port":port}]})).unwrap();
    state.publish("tenant", &template).await.unwrap();
    let scope = Scope {
        tenant: "tenant".into(),
        template: "demo".into(),
        version: "1".into(),
        binding_id: "jiuwen".into(),
    };
    let binding = state.create_binding(scope.clone()).await.unwrap();
    let route: crate::common::route::RouteInfo = serde_json::from_value(serde_json::json!({"instanceID":binding.sandbox_id,"instanceStatus":{"code":3},"tenantID":"tenant","sandboxID":"runtime-jiuwen","sandboxIP":"127.0.0.1","nodeProxyAddress":address.to_string()})).unwrap();
    node.activate_route(
        route.instance_id.clone(),
        route.sandbox_id.clone(),
        route.sandbox_ip.parse().unwrap(),
    )
    .await;
    routes.put(route.clone());
    let reconcile = tokio::spawn(gateway.clone().run_route_reconciler(routes.subscribe()));
    let access = super::super::agent_service::AgentV2Access::new(Arc::new(
        adx_agent_api::managed::ManagedService::new(Arc::new(
            remote_control(adx_activator::Activator::new(
                state,
                Arc::new(Backend::default()),
            ))
            .await,
        )),
    ));
    let selector = ServiceSelector {
        protocol: Protocol::Ws,
        port: None,
    };
    let selection = access
        .select(
            Arc::new(RequestContext::new(Duration::from_secs(3))),
            scope.clone(),
            selector,
        )
        .await
        .unwrap();
    let duplicate = selection.clone();
    let connecting = {
        let access = access.clone();
        let gateway = gateway.clone();
        tokio::spawn(async move { Connection::connect(&gateway, &access, selection).await })
    };
    let (stream, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
        .await
        .unwrap()
        .unwrap();
    // A second frontend can finish its handshake while the first is pending.
    let second_connecting = {
        let access = access.clone();
        let gateway = gateway.clone();
        let selection = duplicate.clone();
        tokio::spawn(async move { Connection::connect(&gateway, &access, selection).await })
    };
    let (second_stream, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
        .await
        .expect("same binding must admit a second backend stream")
        .unwrap();
    let mut second_backend = tokio_tungstenite::accept_async(second_stream)
        .await
        .unwrap();
    let second_connection = second_connecting.await.unwrap().unwrap();
    // tungstenite's Callback API requires an unboxed HTTP error response.
    #[allow(clippy::result_large_err)]
    let inspect_handshake =
        |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
            assert_eq!(request.uri().path(), "/");
            assert!(request.headers().get("authorization").is_none());
            assert!(request.headers().get("x-auth").is_none());
            assert_eq!(
                request.headers()["origin"],
                format!("http://127.0.0.1:{port}")
            );
            Ok(response)
        };
    let mut backend = tokio_tungstenite::accept_hdr_async(stream, inspect_handshake)
        .await
        .unwrap();
    let connection = connecting.await.unwrap().unwrap();
    let (send, requests) = mpsc::channel(2);
    let (output, mut frames) = mpsc::channel(4);
    let running = tokio::spawn(connection.run(
        "user".into(),
        DriverConfig {
            limits: SessionLimits {
                pending: 4,
                recent: 4,
                busy: 4,
            },
            write_timeout: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(5),
            ping_interval: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
        },
        requests,
        output,
    ));
    backend.send(Message::Text(serde_json::json!({"type":"event","event":"connection.ack","payload":{"readiness":"WARMING"}}).to_string())).await.unwrap();
    assert_eq!(frames.recv().await.unwrap()["event"], "connection.ack");
    let (second_send, second_requests) = mpsc::channel(2);
    let (second_output, mut second_frames) = mpsc::channel(4);
    let second_running = tokio::spawn(second_connection.run(
        "user".into(),
        DriverConfig {
            limits: SessionLimits {
                pending: 4,
                recent: 4,
                busy: 4,
            },
            write_timeout: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(5),
            ping_interval: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
        },
        second_requests,
        second_output,
    ));
    second_backend.send(Message::Text(serde_json::json!({"type":"event","event":"connection.ack","payload":{"readiness":"WARMING"}}).to_string())).await.unwrap();
    assert_eq!(
        second_frames.recv().await.unwrap()["event"],
        "connection.ack"
    );
    assert!(frames.try_recv().is_err(), "ACK must stay on its frontend");
    second_send
        .send(
            crate::ingress::jiuwen::protocol::Request::parse(
                br#"{"type":"req","id":"first","method":"session.list","params":{}}"#,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let message = second_backend
        .next()
        .await
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap();
    let envelope: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(envelope["user_id"], "user");
    assert_eq!(envelope["method"], "session.list");
    assert_eq!(gateway.active_sessions(), 2);

    // A third connection is admitted while both drivers are running. Dropping it
    // closes only its own Relay stream; existing frontends continue independently.
    let third_connecting = {
        let access = access.clone();
        let gateway = gateway.clone();
        tokio::spawn(async move { Connection::connect(&gateway, &access, duplicate).await })
    };
    let (third_stream, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let third_backend = tokio_tungstenite::accept_async(third_stream).await.unwrap();
    let third_connection = third_connecting.await.unwrap().unwrap();
    assert_eq!(gateway.active_sessions(), 3);
    drop(third_connection);
    drop(third_backend);
    assert_eq!(gateway.active_sessions(), 2);
    second_running.abort();
    assert!(second_running.await.unwrap_err().is_cancelled());
    drop(second_backend);
    assert!(second_frames.recv().await.is_none());
    assert_eq!(gateway.active_sessions(), 1);
    // The same request ID on the surviving frontend has independent state.
    for id in ["first", "second"] {
        send.send(
            crate::ingress::jiuwen::protocol::Request::parse(
                serde_json::json!({"type":"req","id":id,"method":"session.list","params":{}})
                    .to_string()
                    .as_bytes(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let message = backend.next().await.unwrap().unwrap().into_text().unwrap();
        let envelope: serde_json::Value = serde_json::from_str(&message).unwrap();
        assert_eq!(envelope["user_id"], "user");
        assert_eq!(envelope["method"], "session.list");
    }
    assert_eq!(gateway.active_sessions(), 1);
    routes.delete(&binding.sandbox_id);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), running)
            .await
            .unwrap()
            .unwrap(),
        Err(ConnectionError::Revoked)
    ));
    assert!(frames.recv().await.is_none());
    assert_eq!(gateway.active_sessions(), 0);
    assert!(send
        .send(
            crate::ingress::jiuwen::protocol::Request::parse(
                br#"{"type":"req","id":"late","method":"session.list","params":{}}"#
            )
            .unwrap()
        )
        .await
        .is_err());
    drop(backend);
    // A stalled HTTP upgrade consumes the original admission budget and is not retried.
    routes.put(route.clone());
    let selection = access
        .select(
            Arc::new(RequestContext::new(Duration::from_millis(200))),
            scope.clone(),
            selector,
        )
        .await
        .unwrap();
    let connecting = {
        let access = access.clone();
        let gateway = gateway.clone();
        tokio::spawn(async move { Connection::connect(&gateway, &access, selection).await })
    };
    let (_stalled, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        connecting.await.unwrap(),
        Err(ConnectionError::HandshakeTimeout)
    ));
    assert_eq!(gateway.active_sessions(), 0);
    // Every exit path must release its own stream.
    for exit in [
        "cancel-handshake",
        "bad-handshake",
        "drop",
        "cancel-driver",
        "close-driver",
        "drop",
    ] {
        let selection = access
            .select(
                Arc::new(RequestContext::new(Duration::from_secs(3))),
                scope.clone(),
                selector,
            )
            .await
            .unwrap();
        let connecting = {
            let access = access.clone();
            let gateway = gateway.clone();
            tokio::spawn(async move { Connection::connect(&gateway, &access, selection).await })
        };
        let (mut stream, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
            .await
            .unwrap()
            .unwrap();
        if exit == "cancel-handshake" {
            connecting.abort();
            assert!(matches!(connecting.await, Err(error) if error.is_cancelled()));
        } else if exit == "bad-handshake" {
            use tokio::io::AsyncWriteExt;
            stream
                .write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
            assert!(matches!(
                connecting.await.unwrap(),
                Err(ConnectionError::Driver(_))
            ));
        } else {
            let mut backend = tokio_tungstenite::accept_async(stream).await.unwrap();
            let connection = connecting.await.unwrap().unwrap();
            if exit == "drop" {
                drop(connection);
            } else {
                let (_send, requests) = mpsc::channel(2);
                let (output, mut frames) = mpsc::channel(4);
                let running = tokio::spawn(connection.run(
                    "user".into(),
                    DriverConfig {
                        limits: SessionLimits {
                            pending: 4,
                            recent: 4,
                            busy: 4,
                        },
                        write_timeout: Duration::from_secs(1),
                        idle_timeout: Duration::from_secs(5),
                        ping_interval: Duration::from_secs(1),
                        request_timeout: Duration::from_secs(1),
                    },
                    requests,
                    output,
                ));
                backend.send(Message::Text(serde_json::json!({"type":"event","event":"connection.ack","payload":{"readiness":"WARMING"}}).to_string())).await.unwrap();
                assert_eq!(frames.recv().await.unwrap()["event"], "connection.ack");
                if exit == "cancel-driver" {
                    running.abort();
                    assert!(running.await.unwrap_err().is_cancelled());
                } else {
                    backend.close(None).await.unwrap();
                    assert!(tokio::time::timeout(Duration::from_secs(2), running)
                        .await
                        .unwrap()
                        .unwrap()
                        .is_err());
                }
            }
        }
        assert_eq!(gateway.active_sessions(), 0, "{exit} leaked its stream");
    }
    // A published route belonging to another tenant never reaches the backend.
    let mut foreign = route;
    foreign.tenant_id = "other".into();
    routes.put(foreign);
    let selection = access
        .select(
            Arc::new(RequestContext::new(Duration::from_secs(1))),
            scope,
            selector,
        )
        .await
        .unwrap();
    assert!(matches!(
        Connection::connect(&gateway, &access, selection).await,
        Err(ConnectionError::Access(
            super::super::agent_service::ServiceAccessError::Ingress(IngressOpenError::Forbidden)
        ))
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
    reconcile.abort();
    node_task.abort();
}

#[tokio::test]
async fn jiuwen_download_config_reads_the_scoped_template_without_activation() {
    use crate::ingress::jiuwen::download_config::DownloadConfig;
    use adx_agent_api::{managed::ManagedService, request::RequestContext};
    use adx_agent_core::Scope;
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let backend = Arc::new(Backend::default());
    let managed = ManagedService::new(Arc::new(
        remote_control(adx_activator::Activator::new(
            state.clone(),
            backend.clone(),
        ))
        .await,
    ));
    let mut template:adx_agent_core::TemplateVersion=serde_json::from_value(serde_json::json!({"name":"jiuwen","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"env":{"JIUWENSWARM_WORKSPACE":"/tenant-one/v1","JIUWENSWARM_DOWNLOAD_ASSET_ROOT":"/assets"}})).unwrap();
    state.publish("tenant", &template).await.unwrap();
    template.version = "2".into();
    template
        .env
        .insert("JIUWENSWARM_WORKSPACE".into(), "/tenant-one/v2".into());
    state.publish("tenant", &template).await.unwrap();
    template.version = "1".into();
    template
        .env
        .insert("JIUWENSWARM_WORKSPACE".into(), "/tenant-two/v1".into());
    state.publish("other", &template).await.unwrap();
    let context = RequestContext::new(std::time::Duration::from_secs(2));
    for (tenant, version, workspace) in [
        ("tenant", "1", "/tenant-one/v1"),
        ("tenant", "2", "/tenant-one/v2"),
        ("other", "1", "/tenant-two/v1"),
    ] {
        let scope = Scope {
            tenant: tenant.into(),
            template: "jiuwen".into(),
            version: version.into(),
            binding_id: "not-created".into(),
        };
        let config = DownloadConfig::load(&managed, &context, &scope)
            .await
            .unwrap();
        assert_eq!(config.workspace(), workspace);
        assert!(state.binding(&scope).await.unwrap().is_none());
    }
    assert_eq!(backend.1.load(Ordering::SeqCst), 0);
    assert!(backend.0.lock().unwrap().is_empty());
    let expired = RequestContext::new(std::time::Duration::from_millis(1));
    tokio::time::sleep_until(expired.deadline()).await;
    let scope = Scope {
        tenant: "tenant".into(),
        template: "jiuwen".into(),
        version: "1".into(),
        binding_id: "not-created".into(),
    };
    assert!(
        DownloadConfig::load(&managed, &expired, &scope)
            .await
            .is_err(),
        "cached config still obeys the caller deadline"
    );
}

#[cfg(feature = "mock-e2e")]
#[path = "jiuwen_download_tests.rs"]
mod jiuwen_download_tests;

#[cfg(feature = "mock-e2e")]
#[path = "jiuwen_http_tests.rs"]
mod jiuwen_http_tests;

#[cfg(feature = "mock-e2e")]
#[path = "jiuwen_entrypoint_tests.rs"]
mod jiuwen_entrypoint_tests;
