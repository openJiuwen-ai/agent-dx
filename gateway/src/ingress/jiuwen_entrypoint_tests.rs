//! Public HTTP/WS adapter through the actual Ingress dispatcher and Relay.
use super::*;
use crate::{
    common::protocol::GatewayPolicy,
    ingress::jiuwen::entrypoint::{JiuwenApi, JiuwenConfig},
    node::Relay,
};
use adx_agent_api::{local::LocalControl, managed::ManagedService};
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::time::Duration;
use tokio_tungstenite::{
    tungstenite::{protocol::Role, Message},
    WebSocketStream,
};

fn settings() -> JiuwenConfig {
    serde_json::from_value(json!({"tenant":"tenant","agent_type":"assistant","template":"demo","version":"1","allowed_hosts":["localhost"],"allowed_origins":["https://localhost"]})).unwrap()
}
fn upgrade_request() -> Request<Full<Bytes>> {
    Request::get("/ws")
        .header("host", "localhost")
        .header("authorization", "Bearer alice-session")
        .header("connection", "Upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .body(Full::new(Bytes::new()))
        .unwrap()
}
async fn ws_frame<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    socket: &mut WebSocketStream<S>,
) -> Value {
    loop {
        match tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(_) => socket.flush().await.unwrap(),
            other => panic!("unexpected {other:?}"),
        }
    }
}

#[tokio::test]
async fn jiuwen_auth_is_transport_independent_while_management_requires_tls() {
    let (gateway, backend) = fixture().await;
    let managed = Arc::new(ManagedService::new(Arc::new(LocalControl::new(Arc::new(
        adx_activator::Activator::new(
            AgentState::new(Arc::new(MemoryRepository::default())),
            backend,
        ),
    )))));
    let gateway = Arc::new(
        Arc::try_unwrap(gateway)
            .ok()
            .unwrap()
            .with_agent_api(Arc::new(AgentApi {
                managed: managed.clone(),
                request_timeout: adx_agent_core::limits::AGENT_REQUEST_TIMEOUT,
            }))
            .with_jiuwen_api(Arc::new(
                JiuwenApi::new(
                    settings(),
                    managed,
                    Arc::new(TestAuth(std::sync::atomic::AtomicBool::new(false))),
                )
                .unwrap(),
            )),
    );
    // The duplex fixture tests dispatcher policy, not a real TLS handshake.
    for security in [IngressSecurity::Plaintext, IngressSecurity::Tls] {
        let (mut sender, server, client) = connection(gateway.clone(), security).await;
        let mut spoofed = upgrade_request();
        spoofed.headers_mut().remove("authorization");
        spoofed
            .headers_mut()
            .insert("x-forwarded-proto", "https".parse().unwrap());
        let response = sender.send_request(spoofed).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        response.collect().await.unwrap();

        // Valid business credentials reach protocol validation in either mode,
        // without activating a Sandbox or opening a backend connection.
        let mut invalid_upgrade = upgrade_request();
        invalid_upgrade
            .headers_mut()
            .insert("sec-websocket-version", "12".parse().unwrap());
        let response = sender.send_request(invalid_upgrade).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        response.collect().await.unwrap();

        // Business credentials must not authorize management requests. A scheme
        // header also cannot promote a plaintext request to the TLS listener.
        let response = sender
            .send_request(
                Request::get("/api/agent/v2/templates")
                    .header("authorization", "Bearer alice-session")
                    .header("x-forwarded-proto", "https")
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            match security {
                IngressSecurity::Plaintext => StatusCode::UPGRADE_REQUIRED,
                IngressSecurity::Tls => StatusCode::UNAUTHORIZED,
            }
        );
        response.collect().await.unwrap();
        server.abort();
        client.abort();
    }
}

#[tokio::test]
async fn jiuwen_public_routes_share_one_e2a_and_download_on_another_replica() {
    let security = IngressSecurity::Plaintext;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (backend_sender, mut backends) = tokio::sync::mpsc::channel(4);
    let app = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let sender = backend_sender.clone();
            tokio::spawn(async move {
                let _=hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream),service_fn(move |mut request:Request<Incoming>| {
                    let sender=sender.clone(); async move {
                        if request.uri().path()=="/" {
                            assert!(!request.headers().contains_key("authorization"));
                            let key=request.headers()["sec-websocket-key"].as_bytes();
                            let accept=tokio_tungstenite::tungstenite::handshake::derive_accept_key(key);
                            let upgrade=hyper::upgrade::on(&mut request);
                            tokio::spawn(async move {
                                let mut socket=WebSocketStream::from_raw_socket(TokioIo::new(upgrade.await.unwrap()),Role::Server,None).await;
                                socket.send(Message::Text(json!({"type":"event","event":"connection.ack","payload":{"status":"ready"}}).to_string())).await.unwrap();
                                sender.send(socket).await.unwrap();
                            });
                            return Ok::<_,Infallible>(Response::builder().status(101).header("connection","Upgrade").header("upgrade","websocket").header("sec-websocket-accept",accept).body(Full::new(Bytes::new())).unwrap());
                        }
                        assert_eq!(request.headers()["x-auth"],"runtime-only-secret");
                        if request.uri().path()=="/upload" {
                            let query:std::collections::BTreeMap<_,_>=url::form_urlencoded::parse(request.uri().query().unwrap().as_bytes()).into_owned().collect();
                            assert_eq!(query["path"],"/work/uploads/digitalmate/task/upload.txt");
                            assert_eq!(query["type"],"file");
                            let bytes=request.into_body().collect().await.unwrap().to_bytes();
                            assert_eq!(bytes.as_ref(),b"upload-data");
                            assert_eq!(query["offset"],"0");
                            assert!(!query["uploadId"].is_empty());
                            return Ok(Response::new(Full::new(Bytes::from(json!({"error":null,"path":query["path"],"offset":bytes.len(),"committed":false}).to_string()))));
                        }
                        if request.uri().path()=="/upload/commit" {
                            let query:std::collections::BTreeMap<_,_>=url::form_urlencoded::parse(request.uri().query().unwrap().as_bytes()).into_owned().collect();
                            assert_eq!(query["path"],"/work/uploads/digitalmate/task/upload.txt");
                            assert_eq!(query["totalSize"],"11");
                            assert!(!query["uploadId"].is_empty());
                            return Ok(Response::new(Full::new(Bytes::from(json!({"error":null,"path":query["path"],"bytes_written":11,"committed":true}).to_string()))));
                        }
                        let response=if request.uri().path()=="/invoke" {
                            Response::new(Full::new(Bytes::from_static(br#"{"type":"file","size":5,"error":null}"#)))
                        } else {
                            assert_eq!(request.uri().path(),"/download");
                            assert_eq!(request.headers()["range"],"bytes=0-4");
                            Response::builder().status(206).header("content-range","bytes 0-4/5").body(Full::new(Bytes::from_static(b"hello"))).unwrap()
                        }; Ok(response)
                    }
                })).with_upgrades().await;
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
    let backend = Arc::new(RuntimeBackend {
        queries: std::sync::atomic::AtomicUsize::new(0),
        inner: Backend::default(),
        port,
    });
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    state.publish("tenant", &serde_json::from_value(json!({"name":"demo","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"ws","port":port}],"env":{"JIUWENSWARM_WORKSPACE":"/work","JIUWENSWARM_DOWNLOAD_ASSET_ROOT":"/assets","JIUWENSWARM_FILE_DOWNLOAD_SECRET":"s".repeat(32)}})).unwrap()).await.unwrap();
    let binding = state
        .create_binding(settings().scope("alice").unwrap())
        .await
        .unwrap();
    let managed = Arc::new(ManagedService::new(Arc::new(LocalControl::new(Arc::new(
        adx_activator::Activator::new(state, backend.clone()),
    )))));
    let route:crate::common::route::RouteInfo=serde_json::from_value(json!({"instanceID":binding.sandbox_id,"instanceStatus":{"code":3},"tenantID":"tenant","sandboxID":"runtime-local","sandboxIP":"127.0.0.1","nodeProxyAddress":address.to_string()})).unwrap();
    relay
        .activate_route(
            route.instance_id.clone(),
            route.sandbox_id.clone(),
            route.sandbox_ip.parse().unwrap(),
        )
        .await;
    let auth = Arc::new(TestAuth(std::sync::atomic::AtomicBool::new(false)));
    let mut gateways = vec![];
    for _ in 0..2 {
        let (gateway, _, routes) = fixture_with_routes().await;
        routes.put(route.clone());
        let mut configured = with_files(&gateway, backend.clone());
        configured.default_direct_port = port;
        gateways.push(Arc::new(configured.with_jiuwen_api(Arc::new(
            JiuwenApi::new(settings(), managed.clone(), auth.clone()).unwrap(),
        ))));
    }
    let (mut sender, server, client) = connection(gateways[0].clone(), security).await;
    // Scheme headers do not grant access; the deployment owns transport security.
    let mut spoofed = upgrade_request();
    spoofed.headers_mut().remove("authorization");
    spoofed
        .headers_mut()
        .insert("x-forwarded-proto", "https".parse().unwrap());
    assert_eq!(
        sender.send_request(spoofed).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let mut wrong_host = upgrade_request();
    wrong_host
        .headers_mut()
        .insert("host", "untrusted.example".parse().unwrap());
    assert_eq!(
        sender.send_request(wrong_host).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let login = Request::post("/auth/huawei/login")
        .header("host", "localhost")
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from_static(
            br#"{"authorizationCode":"test","agreementVersion":"1"}"#,
        )))
        .unwrap();
    assert_eq!(
        sender.send_request(login).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let logout = Request::post("/auth/logout")
        .header("host", "localhost")
        .body(Full::new(Bytes::new()))
        .unwrap();
    assert_eq!(
        sender.send_request(logout).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let mut missing = upgrade_request();
    *missing.uri_mut() = "/ws?user_id=alice".parse().unwrap();
    missing.headers_mut().remove("authorization");
    assert_eq!(
        sender.send_request(missing).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    for query in [
        "tenant=other".to_string(),
        format!("user_id={}", "x".repeat(1024)),
    ] {
        let mut invalid_query = upgrade_request();
        *invalid_query.uri_mut() = format!("/ws?{query}").parse().unwrap();
        assert_eq!(
            sender.send_request(invalid_query).await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
    }
    let mut denied = upgrade_request();
    denied
        .headers_mut()
        .insert("origin", "https://untrusted.example".parse().unwrap());
    assert_eq!(
        sender.send_request(denied).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let mut invalid_upgrade = upgrade_request();
    invalid_upgrade
        .headers_mut()
        .insert("sec-websocket-version", "12".parse().unwrap());
    assert_eq!(
        sender.send_request(invalid_upgrade).await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );
    assert!(backends.try_recv().is_err());
    // Client identity hints, including repeated/empty values, never select a user.
    let mut request = upgrade_request();
    *request.uri_mut() = "/ws?user_id=bob&user_id=&user_id=someone-else"
        .parse()
        .unwrap();
    let response = sender.send_request(request).await.unwrap();
    assert_eq!(response.status(), 101);
    let mut frontend = WebSocketStream::from_raw_socket(
        TokioIo::new(hyper::upgrade::on(response).await.unwrap()),
        Role::Client,
        None,
    )
    .await;
    let mut agent = backends.recv().await.unwrap();
    assert_eq!(ws_frame(&mut frontend).await["event"], "connection.ack");
    // Two frontends with the same authenticated binding each get a backend stream.
    let (mut concurrent, concurrent_server, concurrent_client) =
        connection(gateways[0].clone(), security).await;
    let response = concurrent.send_request(upgrade_request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    let mut concurrent_frontend = WebSocketStream::from_raw_socket(
        TokioIo::new(hyper::upgrade::on(response).await.unwrap()),
        Role::Client,
        None,
    )
    .await;
    let mut concurrent_agent = backends.recv().await.unwrap();
    assert_eq!(
        ws_frame(&mut concurrent_frontend).await["event"],
        "connection.ack"
    );
    assert_eq!(gateways[0].active_sessions(), 2);
    for id in ["one", "two"] {
        frontend
            .send(Message::Text(
                json!({"type":"req","id":id,"method":"session.list","params":{"user_id":"forged"}})
                    .to_string(),
            ))
            .await
            .unwrap();
        let envelope = ws_frame(&mut agent).await;
        assert_eq!(envelope["user_id"], "alice");
        // Reusing a client request ID on another frontend must not share results.
        concurrent_frontend
            .send(Message::Text(
                json!({"type":"req","id":id,"method":"session.list","params":{}}).to_string(),
            ))
            .await
            .unwrap();
        let concurrent_envelope = ws_frame(&mut concurrent_agent).await;
        assert_eq!(concurrent_envelope["user_id"], "alice");
        concurrent_agent.send(Message::Text(json!({"protocol_version":"1.0","request_id":concurrent_envelope["request_id"],"sequence":0,"is_final":true,"status":"succeeded","response_kind":"e2a.complete","body":{"result":{"sessions":[],"total":2}}}).to_string())).await.unwrap();
        let concurrent_response = ws_frame(&mut concurrent_frontend).await;
        assert_eq!(concurrent_response["id"], id);
        assert_eq!(concurrent_response["payload"]["total"], 2);
        agent.send(Message::Text(json!({"protocol_version":"1.0","request_id":envelope["request_id"],"sequence":0,"is_final":true,"status":"succeeded","response_kind":"e2a.complete","body":{"result":{"sessions":[],"total":0}}}).to_string())).await.unwrap();
        let response = ws_frame(&mut frontend).await;
        assert_eq!(response["id"], id);
        assert_eq!(response["payload"]["total"], 0);
    }
    concurrent_frontend.close(None).await.unwrap();
    drop(concurrent_frontend);
    drop(concurrent_agent);
    let claims = base64::engine::general_purpose::URL_SAFE
        .encode(br#"{"path":"/work/report.txt","sid":"s"}"#);
    let signature = ring::hmac::sign(
        &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b"ssssssssssssssssssssssssssssssss"),
        claims.as_bytes(),
    );
    let token = format!("{claims}.{}", hex::encode(signature));
    let (mut download, download_server, download_client) =
        connection(gateways[1].clone(), security).await;
    for (query, expected) in [
        (
            format!("token={token}&user_id=other-user"),
            StatusCode::FORBIDDEN,
        ),
        ("token=".to_string(), StatusCode::BAD_REQUEST),
    ] {
        let response = download
            .send_request(
                Request::get(format!("/file-api/download?{query}"))
                    .header("host", "localhost")
                    .header("authorization", "Bearer alice-session")
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
    }
    let response = download
        .send_request(
            Request::get(format!("/file-api/download?token={token}&user_id=alice"))
                .header("host", "localhost")
                .header("authorization", "Bearer alice-session")
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "hello"
    );
    assert!(
        backends.try_recv().is_err(),
        "downloads must not create another E2A stream"
    );
    let boundary = "adx-multipart-test";
    let multipart = Bytes::from(format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"upload.txt\"\r\nContent-Type: text/plain\r\n\r\nupload-data\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"session_id\"\r\n\r\ns\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"dir\"\r\n\r\nuploads/digitalmate/task\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"user_id\"\r\n\r\nalice\r\n--{boundary}--\r\n"
    ));
    for uri in ["/file-api/upload", "/file-api/upload?token=ignored"] {
        let queries = backend.queries.load(std::sync::atomic::Ordering::SeqCst);
        let response = download
            .send_request(
                Request::post(uri)
                    .header("host", "localhost")
                    .header("authorization", "Bearer alice-session")
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={boundary}"),
                    )
                    .body(Full::new(multipart.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let result: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(
            result,
            json!({"ok":true,"files":[{"filename":"upload.txt","path":"/work/uploads/digitalmate/task/upload.txt","mime_type":"text/plain","size_bytes":11}],"errors":[]})
        );
        assert_eq!(
            backend.queries.load(std::sync::atomic::Ordering::SeqCst) - queries,
            1,
            "upload and commit share directory admission"
        );
    }
    assert!(
        backends.try_recv().is_err(),
        "upload must not create another E2A stream"
    );
    // The ordinary Sandbox direct entry uses the same deployment credential;
    // a client supplies only its management identity, never the Execd secret.
    if security == IngressSecurity::Tls {
        let response = download
            .send_request(
                Request::get(format!(
                    "/direct/{}/download?path=/work/report.txt&type=file",
                    binding.sandbox_id
                ))
                .header("authorization", "Bearer tenant")
                .header("range", "bytes=0-4")
                .body(Full::new(Bytes::new()))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "hello"
        );
        let response = download
            .send_request(
                Request::get(format!(
                    "/direct/{}/download?path=/work/report.txt&type=file",
                    binding.sandbox_id
                ))
                .header("authorization", "Bearer other")
                .header("range", "bytes=0-4")
                .body(Full::new(Bytes::new()))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
    auth.0.store(true, std::sync::atomic::Ordering::SeqCst);
    frontend
        .send(Message::Text(
            json!({"type":"req","id":"after-logout","method":"session.list","params":{}})
                .to_string(),
        ))
        .await
        .unwrap();
    let ended = tokio::time::timeout(Duration::from_secs(3), frontend.next())
        .await
        .unwrap();
    assert!(matches!(ended, None | Some(Ok(Message::Close(_)))));
    assert!(backends.try_recv().is_err());
    // Closing either side releases its stream; requests are not replayed.
    let _ = agent.close(None).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while gateways[0].active_sessions() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for task in [server, concurrent_server, download_server, relay_task, app] {
        task.abort();
    }
    for task in [client, concurrent_client, download_client] {
        task.abort();
    }
}

struct TestAuth(std::sync::atomic::AtomicBool);
#[async_trait::async_trait]
impl crate::ingress::accounts::BusinessAuth for TestAuth {
    async fn login(
        &self,
        _: crate::ingress::accounts::LoginRequest,
    ) -> crate::ingress::accounts::Result<crate::ingress::accounts::LoginResponse> {
        Err(crate::ingress::accounts::Error::Unauthorized)
    }
    async fn authenticate(
        &self,
        token: &str,
    ) -> crate::ingress::accounts::Result<crate::ingress::accounts::Principal> {
        if token != "alice-session" {
            return Err(crate::ingress::accounts::Error::Unauthorized);
        }
        Ok(crate::ingress::accounts::Principal {
            tenant: "tenant".into(),
            user_id: "alice".into(),
            token_digest: "digest".into(),
            expires_at: 4102444800,
        })
    }
    async fn check(
        &self,
        _: &crate::ingress::accounts::Principal,
    ) -> crate::ingress::accounts::Result<()> {
        if self.0.load(std::sync::atomic::Ordering::SeqCst) {
            Err(crate::ingress::accounts::Error::Unauthorized)
        } else {
            Ok(())
        }
    }
    async fn logout(&self, _: &str) -> crate::ingress::accounts::Result<()> {
        Ok(())
    }
    async fn launch_config(
        &self,
        _: &crate::ingress::accounts::Principal,
    ) -> crate::ingress::accounts::Result<adx_agent_core::launch::LaunchConfig> {
        Err(crate::ingress::accounts::Error::Unavailable)
    }
}
