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
        dispatcher: None,
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
    assert!(gateway.ready()); // No Dispatcher has been constructed.
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
    assert_eq!(created.as_object().unwrap().len(), 2);
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

struct DispatchFixture {
    state: AgentState,
    id: String,
    down: AtomicBool,
    seen: Mutex<Vec<adx_agent_core::dispatcher::ResolveRequest>>,
}
#[async_trait::async_trait]
impl adx_agent_api::dispatcher::Dispatch for DispatchFixture {
    async fn resolve(
        &self,
        r: &adx_agent_core::dispatcher::ResolveRequest,
    ) -> adx_agent_api::Result<adx_agent_core::dispatcher::Target> {
        if self.down.load(Ordering::SeqCst) {
            return Err(adx_agent_api::Error::Unavailable(
                "Dispatcher stopped".into(),
            ));
        }
        self.seen.lock().unwrap().push(r.clone());
        self.state
            .reserve_cold_start(&r.scope, &self.id, &format!("adx-{}", self.id))
            .await?;
        self.state.mark_ready(&r.scope.tenant, &self.id).await?;
        Ok(adx_agent_core::dispatcher::Target::from_instance(
            &self
                .state
                .instance(&r.scope.tenant, &self.id)
                .await?
                .unwrap(),
        ))
    }
    async fn release(&self, scope: &adx_agent_core::Scope) -> adx_agent_api::Result<()> {
        let session = self.state.release_session(scope).await?;
        for id in &session.instances {
            self.state.begin_delete(&scope.tenant, id).await?;
            self.state.confirm_deleted(&scope.tenant, id).await?;
        }
        self.state
            .finish_session_delete(scope, &session.generation)
            .await?;
        Ok(())
    }
    async fn release_instance(
        &self,
        scope: &adx_agent_core::Scope,
        id: &str,
    ) -> adx_agent_api::Result<()> {
        self.state.begin_delete(&scope.tenant, id).await?;
        self.state
            .confirm_deleted(&scope.tenant, id)
            .await
            .map_err(Into::into)
    }
}
#[tokio::test]
async fn managed_routes_resolve_services_and_keep_inline_independent_of_dispatcher() {
    let (gateway, inline) = fixture();
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let dispatch = Arc::new(DispatchFixture {
        state: state.clone(),
        id: uuid::Uuid::new_v4().to_string(),
        down: AtomicBool::new(false),
        seen: Mutex::new(vec![]),
    });
    let api = Arc::new(AgentApi {
        inline: inline.inline.clone(),
        managed: Some(Arc::new(adx_agent_api::managed::ManagedService::new(
            state.clone(),
            dispatch.clone(),
        ))),
        dispatcher: None,
    });
    let gateway = Arc::new(
        Arc::try_unwrap(gateway)
            .ok()
            .unwrap()
            .with_agent_api(api.clone()),
    );
    let (mut sender, server, client) = connection(gateway.clone(), IngressSecurity::Tls).await;
    let template = serde_json::json!({"name":"demo","version":"1","image":"app:1","isolation_runtime":"runc","entrypoint":["/app/start"],"resources":{"cpu_millis":1000,"memory_mib":1024},"service":[{"protocol":"http","port":8080},{"protocol":"ws","port":8080},{"protocol":"ssh","port":22}]});
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
    let session = "/api/agent/v2/templates/demo/versions/1/sessions/test";
    let response = sender
        .send_request(request(
            "PUT",
            session,
            "tenant",
            "session",
            serde_json::json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    json(response).await;
    let empty_body = Request::builder()
        .method("PUT")
        .uri(session)
        .header("authorization", "Bearer tenant")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let repeated = sender.send_request(empty_body).await.unwrap();
    assert_eq!(repeated.status(), StatusCode::OK);
    let session_record = json(repeated).await;
    assert_eq!(
        session_record["session"]["instances"],
        serde_json::json!([])
    );
    assert!(dispatch.seen.lock().unwrap().is_empty());
    let response = sender
        .send_request(request(
            "POST",
            &format!("{session}/resolve"),
            "tenant",
            "resolve",
            serde_json::json!({"protocol":"ssh","affinity_key":"sticky"}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let target = json(response).await;
    assert_eq!(target["port"], 22);
    assert_eq!(target["instance_id"], dispatch.id);
    assert_eq!(target["sandbox_id"], format!("adx-{}", dispatch.id));
    let authority = format!("{}:22", target["sandbox_id"].as_str().unwrap());
    let mut connect = Request::builder()
        .method("CONNECT")
        .uri(&authority)
        .body(())
        .unwrap();
    gateway
        .prepare_agent_data(&mut connect, IngressSecurity::Tls)
        .await
        .unwrap();
    assert_eq!(connect.uri().to_string(), authority);

    let mut data = Request::builder()
        .uri("/agent/v2/demo/1/test/http/8080/chat?query=original")
        .header("authorization", "Bearer tenant")
        .header("x-adx-affinity-key", "sticky")
        .body(())
        .unwrap();
    gateway
        .prepare_agent_data(&mut data, IngressSecurity::Tls)
        .await
        .unwrap();
    assert_eq!(
        data.uri().to_string(),
        format!("/adx-{}/8080/chat?query=original", dispatch.id)
    );
    assert!(!data.headers().contains_key("x-adx-affinity-key"));
    assert_eq!(
        dispatch
            .seen
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .affinity_key
            .as_deref(),
        Some("sticky")
    );
    assert!(api.retry_data(&mut data).await.unwrap());
    assert!(dispatch.seen.lock().unwrap().last().unwrap().bypass_cache);
    assert_eq!(
        data.uri().to_string(),
        format!("/adx-{}/8080/chat?query=original", dispatch.id)
    );
    assert!(!api.retry_data(&mut data).await.unwrap());
    // Exercise the complete request path: this fixture has no backend route, so Gateway
    // must re-resolve once, with the original affinity, without consuming/replaying a body.
    let before = dispatch.seen.lock().unwrap().len();
    let req = Request::builder()
        .method("POST")
        .uri("/agent/v2/demo/1/test/http/8080/chat?x=1")
        .header("authorization", "Bearer tenant")
        .header("x-adx-affinity-key", "sticky")
        .body(Full::new(Bytes::from_static(b"business-payload")))
        .unwrap();
    let response = sender.send_request(req).await.unwrap();
    assert!(response.status().is_server_error() || response.status() == StatusCode::NOT_FOUND);
    response.into_body().collect().await.unwrap();
    {
        let seen = dispatch.seen.lock().unwrap();
        assert_eq!(seen.len(), before + 2);
        assert!(!seen[before].bypass_cache);
        assert!(seen[before + 1].bypass_cache);
        assert_eq!(seen[before + 1].affinity_key.as_deref(), Some("sticky"));
    }
    let response = sender
        .send_request(request(
            "DELETE",
            &format!("/api/agent/{}", dispatch.id),
            "tenant",
            "wrong-delete",
            serde_json::Value::Null,
        ))
        .await
        .unwrap();
    // Inline passes this UUID straight to Sandbox. The managed Sandbox uses adx-UUID,
    // so it cannot be deleted through this endpoint and ADX state is untouched.
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    json(response).await;
    assert_eq!(
        state
            .instance("tenant", &dispatch.id)
            .await
            .unwrap()
            .unwrap()
            .phase,
        adx_agent_core::InstancePhase::Ready
    );
    let response = sender
        .send_request(request(
            "GET",
            &format!("{session}/instances/{}", dispatch.id),
            "tenant",
            "get-instance",
            serde_json::Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json(response).await["instance"]["id"], dispatch.id);
    let response = sender
        .send_request(request(
            "GET",
            &format!("{session}/instances/{}", dispatch.id),
            "other",
            "other-instance",
            serde_json::Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    json(response).await;
    dispatch.down.store(true, Ordering::SeqCst);
    let response = sender
        .send_request(request(
            "POST",
            &format!("{session}/resolve"),
            "tenant",
            "down",
            serde_json::json!({"protocol":"http"}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    json(response).await;
    let response = sender
        .send_request(request("POST", "/api/agent", "tenant", "inline", input()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    json(response).await;
    dispatch.down.store(false, Ordering::SeqCst);
    let response = sender
        .send_request(request(
            "DELETE",
            session,
            "tenant",
            "release",
            serde_json::Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json(response).await["status"], "deleted");
    client.abort();
    server.abort();
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
        session_id: "session".into(),
    };
    let template = serde_json::from_value(serde_json::json!({"name":"test","version":"1","image":"app:1","isolation_runtime":"runc","entrypoint":["/start"],"resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080}]})).unwrap();
    state.publish("tenant", &template).await.unwrap();
    state.create_session(scope.clone()).await.unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    state
        .reserve_cold_start(&scope, &id, &format!("adx-{id}"))
        .await
        .unwrap();
    state.mark_ready("tenant", &id).await.unwrap();
    let dispatch = Arc::new(DispatchFixture {
        state: state.clone(),
        id: id.clone(),
        down: AtomicBool::new(false),
        seen: Mutex::new(vec![]),
    });
    gateway = gateway.with_agent_api(Arc::new(AgentApi {
        inline: api.inline.clone(),
        managed: Some(Arc::new(adx_agent_api::managed::ManagedService::new(
            state,
            dispatch.clone(),
        ))),
        dispatcher: None,
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
    assert!(dispatch.seen.lock().unwrap().is_empty());
    let mut request = Request::builder()
        .uri("/agent/v2/test/1/session/http/8080/")
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
    assert_eq!(request.uri().path(), format!("/adx-{id}/8080/"));
    assert_eq!(verifier.0.load(Ordering::SeqCst), 1);
    let mut wrong = Request::builder()
        .uri("/agent/v2/test/1/session/http/8080/")
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
async fn inline_only_initializes_without_redis_or_dispatcher() {
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
