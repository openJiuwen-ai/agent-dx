//! Real Relay transport with a simulated Execd; no live Sandbox claim.
use super::*;
use crate::{
    common::protocol::GatewayPolicy,
    ingress::sandbox_files::{Files, ReadError},
    node::Relay,
};
use adx_agent_api::request::RequestContext;
use futures_util::StreamExt;
use http_body_util::{combinators::BoxBody, StreamBody};
use hyper::body::Frame;
use std::time::Duration;

#[tokio::test]
async fn jiuwen_execd_reads_are_authorized_bounded_and_cancelled() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (observed, mut requests) = tokio::sync::mpsc::unbounded_channel();
    let runtime_task = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let observed = observed.clone();
            tokio::spawn(async move {
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(
                        TokioIo::new(stream),
                        service_fn(move |request: Request<Incoming>| {
                            let observed = observed.clone();
                            async move {
                                assert_eq!(request.headers()["x-auth"], "runtime-only-secret");
                                assert_eq!(request.headers()["host"], "execd.internal");
                                assert_eq!(request.headers()["connection"], "close");
                                let path = if request.uri().path() == "/invoke" {
                                    assert_eq!(request.method(), "POST");
                                    let body =
                                        request.into_body().collect().await.unwrap().to_bytes();
                                    let input: serde_json::Value =
                                        serde_json::from_slice(&body).unwrap();
                                    assert_eq!(input["action"], "fs_get_info");
                                    input["args"]["path"].as_str().unwrap().to_string()
                                } else {
                                    assert_eq!(request.uri().path(), "/download");
                                    assert_eq!(request.method(), "GET");
                                    let query: std::collections::BTreeMap<_, _> =
                                        url::form_urlencoded::parse(
                                            request.uri().query().unwrap().as_bytes(),
                                        )
                                        .into_owned()
                                        .collect();
                                    assert_eq!(query["type"], "file");
                                    query["path"].clone()
                                };
                                observed.send(path.clone()).unwrap();
                                let mut status = StatusCode::OK;
                                let body: BoxBody<Bytes, Infallible> = match path.as_str() {
                                    "/stalled" => BodyExt::boxed(StreamBody::new(
                                        futures_util::stream::once(async {
                                            Ok::<_, Infallible>(Frame::data(Bytes::from_static(
                                                b"a",
                                            )))
                                        })
                                        .chain(futures_util::stream::pending()),
                                    )),
                                    "/oversize" => BodyExt::boxed(StreamBody::new(
                                        futures_util::stream::iter([
                                            Ok::<_, Infallible>(Frame::data(Bytes::from_static(
                                                b"abcd",
                                            ))),
                                            Ok(Frame::data(Bytes::from_static(b"ef"))),
                                        ]),
                                    )),
                                    "/info" | "/data/报告.txt" | "/assets/abababababababababababababababab.pdf" => Full::new(Bytes::from_static(
                                        br#"{"type":"file","size":5,"error":null}"#,
                                    ))
                                    .boxed(),
                                    "/work/config/.file_download_secret" => Full::new(Bytes::from_static(b"ssssssssssssssssssssssssssssssss\n")).boxed(),
                                    "/assets/abababababababababababababababab.json" => Full::new(Bytes::from(serde_json::json!({
                                        "state":"committed", "asset_id":"ab".repeat(16),
                                        "sealed_path":"/assets/abababababababababababababababab.pdf",
                                        "expires_at":4102444800.0, "size_bytes":5,
                                        "content_digest":format!("sha256:{}", "12".repeat(32))
                                    }).to_string())).boxed(),
                                    "/bad-info" => Full::new(Bytes::from_static(
                                        br#"{"type":"file","size":-1,"error":null}"#,
                                    ))
                                    .boxed(),
                                    "/info-error" => Full::new(Bytes::from_static(
                                        br#"{"error":"runtime-only-secret"}"#,
                                    ))
                                    .boxed(),
                                    "/missing" => {
                                        status = StatusCode::NOT_FOUND;
                                        Full::new(Bytes::from_static(b"runtime-only-secret"))
                                            .boxed()
                                    }
                                    _ => Full::new(Bytes::from_static(b"hello")).boxed(),
                                };
                                Ok::<_, Infallible>(
                                    Response::builder().status(status).body(body).unwrap(),
                                )
                            }
                        }),
                    )
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
    let backend = Arc::new(RuntimeBackend {
        queries: std::sync::atomic::AtomicUsize::new(0),
        inner: Backend::default(),
        port,
    });
    let (gateway, api, routes) = fixture_with_backend(backend.clone()).await;
    let gateway = Arc::new(with_files(&gateway, backend.clone()));
    let context = RequestContext::new(Duration::from_secs(30));
    let id = create_sandbox(api.as_ref()).await;
    let route: crate::common::route::RouteInfo = serde_json::from_value(serde_json::json!({"instanceID":id,"instanceStatus":{"code":3},"tenantID":"tenant","sandboxID":"runtime","sandboxIP":"127.0.0.1","nodeProxyAddress":address.to_string()})).unwrap();
    node.activate_route(
        route.instance_id.clone(),
        route.sandbox_id.clone(),
        route.sandbox_ip.parse().unwrap(),
    )
    .await;
    routes.put(route.clone());
    let reconcile = tokio::spawn(gateway.clone().run_route_reconciler(routes.subscribe()));
    let reader = Files::new(&gateway, context.deadline(), "tenant", &id).unwrap();
    assert_eq!(
        reader.read_file("/space & 中文.json", 5).await.unwrap(),
        "hello"
    );
    assert_eq!(requests.recv().await.unwrap(), "/space & 中文.json");
    let metadata = reader.metadata("/info").await.unwrap();
    assert!(metadata.regular_file);
    assert_eq!(metadata.size, 5);
    assert!(matches!(
        reader.metadata("/bad-info").await,
        Err(ReadError::Protocol)
    ));
    let error = reader.metadata("/info-error").await.err().unwrap();
    assert!(!error.to_string().contains("runtime-only-secret"));
    assert!(matches!(
        reader.read_file("/missing", 5).await,
        Err(ReadError::NotFound)
    ));
    assert!(matches!(
        reader.read_file("/oversize", 5).await,
        Err(ReadError::TooLarge)
    ));
    assert!(matches!(
        reader.read_file("/normal", 4).await,
        Err(ReadError::TooLarge)
    ));
    assert!(matches!(
        reader.read_file("/../secret", 5).await,
        Err(ReadError::InvalidPath)
    ));
    assert!(matches!(
        reader.read_file("/normal", usize::MAX).await,
        Err(ReadError::InvalidLimit)
    ));
    assert!(matches!(
        Files::new(&gateway, context.deadline(), "", &id),
        Err(ReadError::Identity)
    ));
    let foreign = Files::new(&gateway, context.deadline(), "other", &id).unwrap();
    assert!(matches!(
        foreign.read_file("/normal", 5).await,
        Err(ReadError::NotFound)
    ));
    // A valid Sandbox lookup cannot bypass a conflicting route's tenant.
    let mut wrong_route = route.clone();
    wrong_route.tenant_id = "other".into();
    routes.put(wrong_route);
    assert!(matches!(
        reader.read_file("/normal", 5).await,
        Err(ReadError::Revoked)
    ));
    let fresh = Files::new(&gateway, context.deadline(), "tenant", &id).unwrap();
    assert!(matches!(
        fresh.read_file("/normal", 5).await,
        Err(ReadError::NotFound)
    ));
    routes.put(route.clone());
    while requests.try_recv().is_ok() {}
    assert_eq!(gateway.active_sessions(), 0);

    // Compose the same-template config, real Relay reads and token/asset checks.
    use crate::ingress::jiuwen::{
        download_config::DownloadConfig, download_runtime::AdmissionError,
        download_token::TokenError,
    };
    let mut template: adx_agent_core::TemplateVersion = serde_json::from_value(serde_json::json!({
        "name":"jiuwen","version":"1","image":"app:1","isolation_runtime":"runc",
        "resources":{"cpu_millis":1000,"memory_mib":512},
        "env":{"JIUWENSWARM_WORKSPACE":"/work","JIUWENSWARM_DOWNLOAD_ASSET_ROOT":"/assets"},
        "service":[{"protocol":"ws","port":18091}]
    }))
    .unwrap();
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/jiuwen_download_tokens.json"
    ))
    .unwrap();
    let ordinary = vectors["vectors"]["ordinary"]["token"].as_str().unwrap();
    let mut payload = vectors["vectors"]["verified"]["payload"].clone();
    payload["exp"] = serde_json::json!(4102444800.0);
    payload["size"] = serde_json::json!(5);
    let sign = |payload: &serde_json::Value| {
        use base64::Engine;
        let body =
            base64::engine::general_purpose::URL_SAFE.encode(serde_json::to_vec(payload).unwrap());
        let mac = ring::hmac::sign(
            &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b"ssssssssssssssssssssssssssssssss"),
            body.as_bytes(),
        );
        format!("{body}.{}", hex::encode(mac))
    };
    let verified = sign(&payload);
    let verifier =
        crate::ingress::jiuwen::download_runtime::Reader::new(&gateway, &context, "tenant", &id)
            .unwrap();
    for explicit in [false, true] {
        if explicit {
            template
                .env
                .insert("JIUWENSWARM_FILE_DOWNLOAD_SECRET".into(), "s".repeat(32));
        }
        let config = DownloadConfig::from_template(&template).unwrap();
        for token in [ordinary, verified.as_str()] {
            let checked = verifier
                .authorize(&config, token, Some("session-a"))
                .await
                .unwrap();
            assert_eq!(checked.size(), 5);
            if !explicit {
                assert_eq!(
                    requests.recv().await.unwrap(),
                    "/work/config/.file_download_secret"
                );
            }
            if token == verified {
                assert_eq!(
                    requests.recv().await.unwrap(),
                    "/assets/abababababababababababababababab.json"
                );
            }
            assert_eq!(requests.recv().await.unwrap(), checked.path());
            assert!(requests.try_recv().is_err());
        }
    }
    let config = DownloadConfig::from_template(&template).unwrap();
    assert!(matches!(
        verifier
            .authorize(&config, "bad.token", Some("session-a"))
            .await,
        Err(AdmissionError::Token(_))
    ));
    assert!(requests.try_recv().is_err());
    payload["size"] = serde_json::json!(6);
    assert!(matches!(
        verifier
            .authorize(&config, &sign(&payload), Some("session-a"))
            .await,
        Err(AdmissionError::Token(TokenError::RegistrationMismatch))
    ));
    while requests.try_recv().is_ok() {}

    let deadline = RequestContext::new(Duration::from_millis(100));
    let short = Files::new(&gateway, deadline.deadline(), "tenant", &id).unwrap();
    assert!(matches!(
        short.read_file("/stalled", 5).await,
        Err(ReadError::Timeout)
    ));
    assert_eq!(requests.recv().await.unwrap(), "/stalled");
    assert_eq!(gateway.active_sessions(), 0);
    assert!(matches!(
        short.read_file("/normal", 5).await,
        Err(ReadError::Timeout)
    ));
    assert!(requests.try_recv().is_err());

    let reading = {
        let gateway = gateway.clone();
        let id = id.clone();
        tokio::spawn(async move {
            let context = RequestContext::new(Duration::from_secs(10));
            Files::new(&gateway, context.deadline(), "tenant", &id)
                .unwrap()
                .read_file("/stalled", 5)
                .await
        })
    };
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), requests.recv())
            .await
            .unwrap()
            .unwrap(),
        "/stalled"
    );
    routes.delete(&id);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), reading)
            .await
            .unwrap()
            .unwrap(),
        Err(ReadError::Revoked)
    ));
    assert_eq!(gateway.active_sessions(), 0);

    // Cancelling the caller must also drop the owned HTTP connection and session.
    routes.put(route);
    let reading = {
        let gateway = gateway.clone();
        let id = id.clone();
        tokio::spawn(async move {
            let context = RequestContext::new(Duration::from_secs(10));
            Files::new(&gateway, context.deadline(), "tenant", &id)
                .unwrap()
                .read_file("/stalled", 5)
                .await
        })
    };
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), requests.recv())
            .await
            .unwrap()
            .unwrap(),
        "/stalled"
    );
    reading.abort();
    assert!(reading.await.unwrap_err().is_cancelled());
    assert_eq!(gateway.active_sessions(), 0);
    reconcile.abort();
    node_task.abort();
    runtime_task.abort();
}
