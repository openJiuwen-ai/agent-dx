//! Transport and fixed-ID mapping checks; no live Platform claim.
use super::*;
use crate::edge::{
    agent_api::AgentApi,
    auth::{AuthError, AuthenticatedIdentity, CredentialVerifier},
    H2PoolConfig, RouteStore,
};
use adx_agent_api::management::{InlineProfile, InlineService, Options};
use adx_agent_core::{
    inline::{CreateRequest, SandboxType},
    sandbox::*,
};
use adx_agent_store::{AgentState, MemoryRepository};
use std::time::Duration;

#[derive(Default)]
struct Backend(Mutex<std::collections::HashMap<(String, String), SandboxObservation>>);
#[async_trait::async_trait]
impl Sandbox for Backend {
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
fn fixture() -> (Arc<EdgeFrontend>, Arc<AgentApi>) {
    let (gateway, api, _) = fixture_with_routes();
    (gateway, api)
}
fn fixture_with_routes() -> (Arc<EdgeFrontend>, Arc<AgentApi>, Arc<RouteStore>) {
    fixture_with_backend(Arc::new(Backend::default()))
}
fn fixture_with_backend(
    backend: Arc<dyn Sandbox>,
) -> (Arc<EdgeFrontend>, Arc<AgentApi>, Arc<RouteStore>) {
    let inline = Arc::new(
        InlineService::new(
            backend,
            Options {
                profiles: vec![InlineProfile {
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
                backend_timeout: Duration::from_secs(1),
                max_inflight: 16,
            },
        )
        .unwrap(),
    );
    let api = Arc::new(AgentApi {
        inline,
        managed: None,
        activator: None,
    });
    let store = Arc::new(RouteStore::new());
    store.set_ready(true);
    let gateway = Arc::new(
        EdgeFrontend::new(
            Arc::new(EdgeRouteResolver::new(store.clone())),
            DataPlaneL4Connector::new(H2PoolConfig::default()),
            EdgeAuthenticator::with_verifier(Arc::new(Identity)),
            50090,
            8765,
            "127.0.0.1:1",
            vec![],
        )
        .with_agent_api(api.clone()),
    );
    (gateway, api, store)
}
fn input() -> serde_json::Value {
    serde_json::json!({"name":"demo","namespace":"default","runtime_spec":{"runtime":"Python3.11","sandbox_type":"docker","rootfs":{"imageurl":"app:1"}}})
}
async fn connection(
    gateway: Arc<EdgeFrontend>,
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
            .await
            .unwrap();
    });
    let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(client))
        .await
        .unwrap();
    (sender, serving, tokio::spawn(connection))
}
fn request(
    method: &str,
    path: &str,
    token: &str,
    trace: &str,
    body: serde_json::Value,
) -> Request<Full<Bytes>> {
    Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .header("x-trace-id", trace)
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap()
}
async fn json(response: Response<Incoming>) -> serde_json::Value {
    serde_json::from_slice(&response.collect().await.unwrap().to_bytes()).unwrap()
}

#[tokio::test]
async fn legacy_inline_http_contract_and_tenant_boundary() {
    let (gateway, _) = fixture();
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
    let (gateway, _) = fixture();
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
    let (gateway, api) = fixture();
    let request: CreateRequest = serde_json::from_value(input()).unwrap();
    let id = api
        .inline
        .create("tenant", request)
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
    api.inline.kill("tenant", &id).await.unwrap();
}

struct ControlFixture {
    inner: adx_activator::Activator,
}
#[async_trait::async_trait]
impl adx_agent_api::activator::Control for ControlFixture {
    async fn publish(
        &self,
        tenant: &str,
        template: &adx_agent_core::TemplateVersion,
    ) -> adx_agent_api::Result<()> {
        self.inner.publish(tenant, template).await
    }
    async fn template(
        &self,
        tenant: &str,
        name: &str,
        version: &str,
    ) -> adx_agent_api::Result<adx_agent_core::TemplateVersion> {
        self.inner.template(tenant, name, version).await
    }
    async fn create_environment(
        &self,
        scope: &adx_agent_core::Scope,
    ) -> adx_agent_api::Result<adx_agent_core::Environment> {
        self.inner.create_environment(scope.clone()).await
    }
    async fn environment(
        &self,
        scope: &adx_agent_core::Scope,
    ) -> adx_agent_api::Result<adx_agent_core::Environment> {
        self.inner.environment(scope).await
    }
    async fn delete_environment(&self, scope: &adx_agent_core::Scope) -> adx_agent_api::Result<()> {
        self.inner.delete_environment(scope).await
    }
    async fn activate(
        &self,
        scope: &adx_agent_core::Scope,
    ) -> adx_agent_api::Result<adx_agent_core::activator::Target> {
        self.inner.activate(scope).await
    }
}
#[tokio::test]
async fn managed_environment_management_and_protocol_forwarding() {
    let (gateway, inline) = fixture();
    let control = Arc::new(ControlFixture {
        inner: adx_activator::Activator::new(
            AgentState::new(Arc::new(MemoryRepository::default())),
            Arc::new(Backend::default()),
        ),
    });
    let api = Arc::new(AgentApi {
        inline: inline.inline.clone(),
        managed: Some(Arc::new(adx_agent_api::managed::ManagedService::new(
            control,
        ))),
        activator: None,
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
    let response = sender
        .send_request(request(
            "PUT",
            path,
            "tenant",
            "create",
            serde_json::json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let environment = json(response).await["environment"].clone();
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
    for protocol in ["http", "ws"] {
        let mut builder = Request::builder()
            .uri(format!("/agent/v2/demo/1/env/{protocol}/8080/path?q=1"))
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
        assert!(api.retry_data(&mut data).await.unwrap());
        assert!(!api.retry_data(&mut data).await.unwrap());
    }
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
    let (gateway, api, routes) = fixture_with_routes();
    // Even an actual Agent ID must not shadow a literal Platform route.
    let id = api
        .inline
        .create("tenant", serde_json::from_value(input()).unwrap())
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
    let (gateway, api) = fixture();
    let mut gateway = Arc::try_unwrap(gateway).ok().unwrap();
    let verifier = Arc::new(CountIdentity(std::sync::atomic::AtomicUsize::new(0)));
    gateway.authenticator = EdgeAuthenticator::with_verifier(verifier.clone());
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
    let control = Arc::new(ControlFixture {
        inner: adx_activator::Activator::new(state.clone(), Arc::new(Backend::default())),
    });
    gateway = gateway.with_agent_api(Arc::new(AgentApi {
        inline: api.inline.clone(),
        managed: Some(Arc::new(adx_agent_api::managed::ManagedService::new(
            control,
        ))),
        activator: None,
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
        .uri("/agent/v2/test/1/env/http/8080/")
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
        .uri("/agent/v2/test/1/env/http/8080/")
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
    let (gateway, _, _) = fixture_with_backend(Arc::new(StalledSandbox));
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
        "mode":"inline_only", "backend_timeout_seconds":2, "max_inflight":4,
        "inline_profiles":[{"sandbox_type":"docker","request_image":"app:1","image":"app:1","isolation_runtime":"runc","working_dir":"/"}]
    })).unwrap();
    let api = AgentApi::new(config, Arc::new(Backend::default()))
        .await
        .unwrap();
    let created = api
        .inline
        .create("tenant", serde_json::from_value(input()).unwrap())
        .await
        .unwrap();
    api.inline
        .kill("tenant", &created.instance_id)
        .await
        .unwrap();
}
