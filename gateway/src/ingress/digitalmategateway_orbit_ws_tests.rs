//! Exercise admission and opaque frames through the shared Ingress and Relay fixture.
use super::*;
use sha2::{Digest, Sha256};

fn orbit_settings(port: u16) -> DigitalMateGatewayConfig {
    serde_json::from_value(json!({
        "tenant":"tenant",
        "jiuwen":{"agent_type":"assistant", "template":"demo", "version":"1"},
        "orbit": {
            "template":"orbit", "version":"fixed", "backend_path":"/opaque", "service_port":port
        }
    }))
    .unwrap()
}

fn orbit_request(query: &str) -> Request<Full<Bytes>> {
    let mut request = upgrade_request();
    *request.uri_mut() = format!("/orbit/ws{query}").parse().unwrap();
    request
}

#[tokio::test]
async fn orbit_admission_requires_session() {
    let (gateway, backend) = fixture().await;
    let managed = Arc::new(ManagedService::new(Arc::new(LocalControl::new(Arc::new(
        adx_activator::Activator::new(
            AgentState::new(Arc::new(MemoryRepository::default())),
            backend,
        ),
    )))));
    for enabled in [false, true] {
        let config = if enabled {
            orbit_settings(18092)
        } else {
            settings()
        };
        let gateway = Arc::new(
            gateway
                .as_ref()
                .clone()
                .with_digitalmategateway_api(Arc::new(
                    DigitalMateGatewayApi::new(
                        config,
                        managed.clone(),
                        Arc::new(TestAuth(std::sync::atomic::AtomicBool::new(false))),
                    )
                    .unwrap(),
                )),
        );
        let (mut sender, server, client) = connection(gateway, IngressSecurity::Plaintext).await;
        for token in [
            None,
            Some("invalid"),
            Some("tenant"),
            Some("expired-session"),
        ] {
            let mut request = orbit_request("");
            request.headers_mut().remove("authorization");
            if let Some(token) = token {
                request
                    .headers_mut()
                    .insert("authorization", format!("Bearer {token}").parse().unwrap());
            }
            let response = sender.send_request(request).await.unwrap();
            assert_eq!(
                response.status(),
                if enabled {
                    StatusCode::UNAUTHORIZED
                } else {
                    StatusCode::NOT_FOUND
                }
            );
            response.collect().await.unwrap();
        }
        server.abort();
        client.abort();
    }
}

#[tokio::test]
async fn orbit_ws_reuses_shared_forwarding_and_preserves_frames() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (backend_sender, mut backends) = tokio::sync::mpsc::channel(8);
    let (hold_sender, mut holds) =
        tokio::sync::mpsc::channel::<tokio::sync::oneshot::Sender<()>>(2);
    let app = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let sender = backend_sender.clone();
            let hold = hold_sender.clone();
            tokio::spawn(async move {
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(
                        TokioIo::new(stream),
                        service_fn(move |mut request: Request<Incoming>| {
                            let sender = sender.clone();
                            let hold = hold.clone();
                            async move {
                                assert_eq!(
                                    request.uri(),
                                    if request.headers().contains_key("x-test-hold") {
                                        "/opaque?mode=opaque"
                                    } else {
                                        "/opaque?mode=opaque&token=discarded"
                                    }
                                );
                                for name in [
                                    "authorization",
                                    "cookie",
                                    "x-auth",
                                    "x-user-id",
                                    H_INSTANCE_ID,
                                    H_WORKLOAD_ID,
                                    H_TARGET_IP,
                                    H_TARGET_PORT,
                                    adx_agent_core::parameters::HEADER,
                                ] {
                                    assert!(!request.headers().contains_key(name), "{name}");
                                }
                                assert_eq!(
                                    request.headers()["sec-websocket-protocol"],
                                    "opaque-test"
                                );
                                if request.headers().contains_key("x-test-hold") {
                                    let (release, wait) = tokio::sync::oneshot::channel();
                                    hold.send(release).await.unwrap();
                                    let _ = wait.await;
                                }
                                let accept =
                                    tokio_tungstenite::tungstenite::handshake::derive_accept_key(
                                        request.headers()["sec-websocket-key"].as_bytes(),
                                    );
                                let upgrade = hyper::upgrade::on(&mut request);
                                tokio::spawn(async move {
                                    let Ok(upgraded) = upgrade.await else {
                                        return;
                                    };
                                    let socket = WebSocketStream::from_raw_socket(
                                        TokioIo::new(upgraded),
                                        Role::Server,
                                        None,
                                    )
                                    .await;
                                    sender.send(socket).await.unwrap();
                                });
                                Ok::<_, Infallible>(
                                    Response::builder()
                                        .status(101)
                                        .header("connection", "Upgrade")
                                        .header("upgrade", "websocket")
                                        .header("sec-websocket-accept", accept)
                                        .header("sec-websocket-protocol", "opaque-test")
                                        .body(Full::new(Bytes::new()))
                                        .unwrap(),
                                )
                            }
                        }),
                    )
                    .with_upgrades()
                    .await;
            });
        }
    });
    let relay = Arc::new(
        Relay::new(GatewayPolicy::for_local_mock(vec!["127.0.0.0/8"
            .parse()
            .unwrap()]))
        .with_route_enforcement(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let relay_task = {
        let relay = relay.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let relay = relay.clone();
                tokio::spawn(async move {
                    let _ = relay.serve_h2(stream).await;
                });
            }
        })
    };
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    state.publish("tenant", &serde_json::from_value(json!({"name":"orbit","version":"fixed","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"ws","port":port}]})).unwrap()).await.unwrap();
    let mut bindings = vec![];
    for user in ["alice", "bob"] {
        bindings.push(
            state
                .create_binding(adx_agent_core::Scope {
                    tenant: "tenant".into(),
                    template: "orbit".into(),
                    version: "fixed".into(),
                    binding_id: format!(
                        "orbit-{}",
                        hex::encode(Sha256::digest(
                            serde_json::to_vec(&["tenant", user]).unwrap()
                        ))
                    ),
                })
                .await
                .unwrap(),
        );
    }
    let backend = Arc::new(Backend::default());
    let managed = Arc::new(ManagedService::new(Arc::new(LocalControl::new(Arc::new(
        adx_activator::Activator::new(state.clone(), backend.clone()),
    )))));
    let (gateway, _, routes) = fixture_with_routes().await;
    for binding in &bindings {
        let route: crate::common::route::RouteInfo = serde_json::from_value(json!({"instanceID":binding.environment_id,"instanceStatus":{"code":3},"tenantID":"tenant","environmentID":format!("runtime-{}",binding.scope.binding_id),"environmentIP":"127.0.0.1","nodeProxyAddress":address.to_string()})).unwrap();
        relay
            .activate_route(
                route.instance_id.clone(),
                route.environment_id.clone(),
                route.environment_ip.parse().unwrap(),
            )
            .await;
        routes.put(route);
    }
    let auth = Arc::new(ModelAuth::new());
    let gateway = Arc::new(
        gateway
            .as_ref()
            .clone()
            .with_digitalmategateway_api(Arc::new(
                DigitalMateGatewayApi::new(orbit_settings(port), managed.clone(), auth.clone())
                    .unwrap(),
            )),
    );
    let reconcile = tokio::spawn(gateway.clone().run_route_reconciler(routes.subscribe()));
    for (index, user) in ["alice", "alice", "bob"].iter().enumerate() {
        let (mut sender, server, client) =
            connection(gateway.clone(), IngressSecurity::Plaintext).await;
        let mut request = orbit_request("?mode=opaque&token=discarded");
        request.headers_mut().insert(
            "authorization",
            format!("Bearer {user}-session").parse().unwrap(),
        );
        request
            .headers_mut()
            .insert("sec-websocket-protocol", "opaque-test".parse().unwrap());
        for name in [
            "cookie",
            "x-user-id",
            H_INSTANCE_ID,
            H_WORKLOAD_ID,
            H_TARGET_IP,
            H_TARGET_PORT,
            adx_agent_core::parameters::HEADER,
        ] {
            request
                .headers_mut()
                .insert(name, "untrusted".parse().unwrap());
        }
        let response = sender.send_request(request).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::SWITCHING_PROTOCOLS,
            "connection {index} user {user}"
        );
        assert!(!response.headers().contains_key("x-adx-binding-id"));
        assert_eq!(response.headers()["sec-websocket-protocol"], "opaque-test");
        let mut front = WebSocketStream::from_raw_socket(
            TokioIo::new(hyper::upgrade::on(response).await.unwrap()),
            Role::Client,
            None,
        )
        .await;
        let mut back = tokio::time::timeout(Duration::from_secs(3), backends.recv())
            .await
            .unwrap()
            .unwrap();
        for message in [
            Message::Text("not JSON".into()),
            Message::Binary(vec![0, 255, 1]),
            Message::Ping(vec![42]),
        ] {
            front.send(message.clone()).await.unwrap();
            assert_eq!(back.next().await.unwrap().unwrap(), message);
        }
        back.flush().await.unwrap();
        assert_eq!(
            front.next().await.unwrap().unwrap(),
            Message::Pong(vec![42])
        );
        back.send(Message::Text("opaque response".into()))
            .await
            .unwrap();
        assert_eq!(
            front.next().await.unwrap().unwrap(),
            Message::Text("opaque response".into())
        );
        if index == 0 {
            let close = Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Policy,
                reason: "backend policy".into(),
            });
            back.close(close.clone()).await.unwrap();
            assert_eq!(front.next().await.unwrap().unwrap(), Message::Close(close));
            front.flush().await.unwrap();
        } else {
            if index == 1 {
                auth.inner
                    .0
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                tokio::time::pause();
                tokio::time::advance(Duration::from_secs(31)).await;
                tokio::time::resume();
            } else {
                routes.delete(&bindings[1].environment_id);
            }
            let end = tokio::time::timeout(Duration::from_secs(3), front.next())
                .await
                .unwrap_or_else(|_| panic!("connection {index} did not close"));
            assert!(end.is_none() || end.unwrap().is_err());
            auth.inner
                .0
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }
        server.abort();
        client.abort();
    }
    // A blocked post-connect account read must consume the original admission budget.
    let mut config = orbit_settings(port);
    config.orbit.as_mut().unwrap().request_timeout_seconds = 1;
    let bounded = Arc::new(
        gateway
            .as_ref()
            .clone()
            .with_digitalmategateway_api(Arc::new(
                DigitalMateGatewayApi::new(config, managed.clone(), auth.clone()).unwrap(),
            )),
    );
    auth.block_checks
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let before = auth.checks.load(std::sync::atomic::Ordering::SeqCst);
    let (mut sender, server, client) = connection(bounded, IngressSecurity::Plaintext).await;
    let pending = tokio::spawn(async move {
        sender
            .send_request(orbit_request("?mode=opaque&token=discarded"))
            .await
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while auth.checks.load(std::sync::atomic::Ordering::SeqCst) < before + 2 {
            auth.checking.notified().await;
        }
    })
    .await
    .expect("admission watcher and post-connect recheck must both start");
    let response = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .expect("a blocked recheck must not extend the one-second budget")
        .unwrap();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    response.collect().await.unwrap();
    auth.block_checks
        .store(false, std::sync::atomic::Ordering::SeqCst);
    server.abort();
    client.abort();

    // A backend that has received Upgrade but has not answered is still admission.
    for route_revoked in [false, true] {
        let (mut sender, server, client) =
            connection(gateway.clone(), IngressSecurity::Plaintext).await;
        let mut request = orbit_request("?mode=opaque");
        request
            .headers_mut()
            .insert("sec-websocket-protocol", "opaque-test".parse().unwrap());
        request
            .headers_mut()
            .insert("x-test-hold", "1".parse().unwrap());
        let pending = tokio::spawn(async move { sender.send_request(request).await.unwrap() });
        let release = tokio::time::timeout(Duration::from_secs(3), holds.recv())
            .await
            .unwrap()
            .unwrap();
        if route_revoked {
            routes.delete(&bindings[0].environment_id);
        } else {
            auth.inner
                .0
                .store(true, std::sync::atomic::Ordering::SeqCst);
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(31)).await;
            tokio::time::resume();
        }
        let response = tokio::time::timeout(Duration::from_secs(3), pending)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            response.status(),
            if route_revoked {
                StatusCode::BAD_GATEWAY
            } else {
                StatusCode::UNAUTHORIZED
            }
        );
        let payload: Value =
            serde_json::from_slice(&response.collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(
            payload["code"],
            if route_revoked {
                "ROUTE_REVOKED"
            } else {
                "UNAUTHORIZED"
            }
        );
        auth.inner
            .0
            .store(false, std::sync::atomic::Ordering::SeqCst);
        drop(release);
        server.abort();
        client.abort();
    }
    for binding in &bindings {
        assert_eq!(
            state
                .binding(&binding.scope)
                .await
                .unwrap()
                .unwrap()
                .environment_id,
            binding.environment_id
        );
    }
    app.abort();
    relay_task.abort();
    reconcile.abort();
}

struct ModelAuth {
    inner: TestAuth,
    calls: std::sync::atomic::AtomicUsize,
    checks: std::sync::atomic::AtomicUsize,
    block_checks: std::sync::atomic::AtomicBool,
    checking: tokio::sync::Notify,
}
impl ModelAuth {
    fn new() -> Self {
        Self {
            inner: TestAuth(std::sync::atomic::AtomicBool::new(false)),
            calls: std::sync::atomic::AtomicUsize::new(0),
            checks: std::sync::atomic::AtomicUsize::new(0),
            block_checks: std::sync::atomic::AtomicBool::new(false),
            checking: tokio::sync::Notify::new(),
        }
    }
}
#[async_trait::async_trait]
impl crate::ingress::accounts::BusinessAuth for ModelAuth {
    async fn login(
        &self,
        request: crate::ingress::accounts::LoginRequest,
    ) -> crate::ingress::accounts::Result<crate::ingress::accounts::LoginResponse> {
        crate::ingress::accounts::BusinessAuth::login(&self.inner, request).await
    }
    async fn authenticate(
        &self,
        token: &str,
    ) -> crate::ingress::accounts::Result<crate::ingress::accounts::Principal> {
        crate::ingress::accounts::BusinessAuth::authenticate(&self.inner, token).await
    }
    async fn check(
        &self,
        principal: &crate::ingress::accounts::Principal,
    ) -> crate::ingress::accounts::Result<()> {
        self.checks
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.checking.notify_one();
        if self.block_checks.load(std::sync::atomic::Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        crate::ingress::accounts::BusinessAuth::check(&self.inner, principal).await
    }
    async fn logout(&self, token: &str) -> crate::ingress::accounts::Result<()> {
        crate::ingress::accounts::BusinessAuth::logout(&self.inner, token).await
    }
    async fn quota(
        &self,
        principal: &crate::ingress::accounts::Principal,
    ) -> crate::ingress::accounts::Result<crate::ingress::accounts::QuotaResponse> {
        crate::ingress::accounts::BusinessAuth::quota(&self.inner, principal).await
    }
    async fn model_parameters(
        &self,
        _: &crate::ingress::accounts::Principal,
    ) -> crate::ingress::accounts::Result<adx_agent_core::mounts::Parameters> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(std::collections::BTreeMap::from([
            ("model_api_key".into(), "synthetic-model-key".into()),
            ("model_api_base".into(), "http://model.test/v1".into()),
        ]))
    }
}

#[tokio::test]
async fn orbit_initialization_uses_account_model_and_user_workspace_parameters() {
    let (gateway, backend) = fixture().await;
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let mut template: adx_agent_core::TemplateVersion = serde_json::from_str(include_str!(
        "../../../agent/activator/examples/orbit-sfs-template.json"
    ))
    .unwrap();
    template.version = "fixed".into();
    state.publish("tenant", &template).await.unwrap();
    let managed = Arc::new(ManagedService::new(Arc::new(LocalControl::new(Arc::new(
        adx_activator::Activator::new(state.clone(), backend),
    )))));
    let auth = Arc::new(ModelAuth::new());
    let gateway = Arc::new(
        gateway
            .as_ref()
            .clone()
            .with_digitalmategateway_api(Arc::new(
                DigitalMateGatewayApi::new(orbit_settings(18092), managed, auth.clone()).unwrap(),
            )),
    );
    let scope = adx_agent_core::Scope {
        tenant: "tenant".into(),
        template: "orbit".into(),
        version: "fixed".into(),
        binding_id: format!(
            "orbit-{}",
            hex::encode(Sha256::digest(
                serde_json::to_vec(&["tenant", "alice"]).unwrap()
            ))
        ),
    };
    let mut identity = None;
    for _ in 0..2 {
        let (mut sender, server, client) =
            connection(gateway.clone(), IngressSecurity::Plaintext).await;
        let mut request = orbit_request("");
        request.headers_mut().insert(
            adx_agent_core::parameters::HEADER,
            "workspace=forged".parse().unwrap(),
        );
        // Activation succeeds; this fixture deliberately publishes no Environment route.
        let response = sender.send_request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        response.collect().await.unwrap();
        let binding = state.binding(&scope).await.unwrap().unwrap();
        assert_eq!(binding.parameters.len(), 3);
        assert_eq!(binding.parameters["model_api_key"], "synthetic-model-key");
        assert_eq!(binding.parameters["model_api_base"], "http://model.test/v1");
        assert_eq!(
            binding.parameters["workspace"],
            format!(
                "w-{}",
                hex::encode(Sha256::digest(
                    serde_json::to_vec(&["tenant", "orbit", "alice"]).unwrap()
                ))
            )
        );
        let mounts = template.render_mounts(&binding.parameters).unwrap();
        assert_eq!(
            mounts[0].host_path,
            format!("/mnt/sfs/workspaces/{}", binding.parameters["workspace"])
        );
        assert_eq!(mounts[0].target, "/data/state");
        let env = template.render_env(&binding.parameters).unwrap();
        assert_eq!(env["API_KEY"], "synthetic-model-key");
        assert_eq!(env["API_BASE"], "http://model.test/v1");
        assert_eq!(env["ORBIT_WORKSPACE"], "/data/state");
        if let Some(previous) = &identity {
            assert_eq!(&binding.environment_id, previous);
        }
        identity = Some(binding.environment_id);
        server.abort();
        client.abort();
    }
    assert_eq!(auth.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}
