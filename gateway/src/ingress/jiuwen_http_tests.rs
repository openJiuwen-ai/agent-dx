//! HTTP download through a real Relay and simulated Execd, without public login wiring.
use super::*;
use crate::{
    common::protocol::GatewayPolicy,
    ingress::jiuwen::{download_config::DownloadConfig, download_http::Download},
    node::Relay,
};
use adx_agent_api::request::RequestContext;
use base64::Engine;
use serde_json::{json as value, Value};
use std::{
    sync::atomic::{AtomicBool, AtomicU8, Ordering},
    time::Duration,
};

const FILE_SIZE: usize = 131075;
const FILE_PATH: &str = "/assets/abababababababababababababababab.pdf";
fn token(expires: f64, verified: bool) -> String {
    let mut claims = value!({"path":FILE_PATH,"sid":"session-a"});
    if verified {
        claims.as_object_mut().unwrap().extend(value!({"kind":"verified_asset_v1","asset_id":"ab".repeat(16),"exp":expires,"size":FILE_SIZE,"digest":format!("sha256:{}","12".repeat(32)),"name":"报告.pdf"}).as_object().unwrap().clone());
    }
    let body =
        base64::engine::general_purpose::URL_SAFE.encode(serde_json::to_vec(&claims).unwrap());
    let signature = ring::hmac::sign(
        &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b"ssssssssssssssssssssssssssssssss"),
        body.as_bytes(),
    );
    format!("{body}.{}", hex::encode(signature))
}
#[tokio::test]
async fn jiuwen_http_streams_ranges_and_rechecks_registration_between_chunks() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let active = Arc::new(AtomicBool::new(true));
    let response_fault = Arc::new(AtomicU8::new(0));
    let ranges = Arc::new(Mutex::new(Vec::<String>::new()));
    let runtime_task = {
        let active = active.clone();
        let response_fault = response_fault.clone();
        let ranges = ranges.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let active = active.clone();
                let response_fault = response_fault.clone();
                let ranges = ranges.clone();
                tokio::spawn(async move {
                    let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream),service_fn(move |request: Request<Incoming>| {
                        let active=active.clone(); let response_fault=response_fault.clone(); let ranges=ranges.clone();
                        async move {
                            assert_eq!(request.headers()["x-auth"],"runtime-only-secret");
                            if request.uri().path()=="/invoke" {
                                let input: Value=serde_json::from_slice(&request.into_body().collect().await.unwrap().to_bytes()).unwrap();
                                assert_eq!(input["action"],"fs_get_info"); assert_eq!(input["args"]["path"],FILE_PATH);
                                return Ok::<_,Infallible>(Response::new(Full::new(Bytes::from(value!({"type":"file","size":FILE_SIZE,"error":null}).to_string()))));
                            }
                            assert_eq!(request.uri().path(),"/download");
                            let query:std::collections::BTreeMap<_,_>=url::form_urlencoded::parse(request.uri().query().unwrap().as_bytes()).into_owned().collect();
                            assert_eq!(query["type"],"file");
                            if query["path"].ends_with(".json") {
                                assert_eq!(query["path"],"/assets/abababababababababababababababab.json");
                                return Ok(Response::new(Full::new(Bytes::from(value!({"state":if active.load(Ordering::SeqCst){"committed"}else{"revoked"},"asset_id":"ab".repeat(16),"sealed_path":FILE_PATH,"expires_at":4102444800.0,"size_bytes":FILE_SIZE,"content_digest":format!("sha256:{}","12".repeat(32))}).to_string()))));
                            }
                            assert_eq!(query["path"],FILE_PATH);
                            let range=request.headers()["range"].to_str().unwrap().to_string();
                            ranges.lock().unwrap().push(range.clone());
                            let (start,end)=range.strip_prefix("bytes=").unwrap().split_once('-').unwrap();
                            let start:usize=start.parse().unwrap(); let end:usize=end.parse().unwrap();
                            assert!(end-start<65536);
                            let fault=response_fault.load(Ordering::SeqCst);
                            let length=match fault {3=>1,4=>end-start+2,_=>end-start+1};
                            Ok(Response::builder().status(if fault==2{200}else{206}).header("content-range", if fault==1{"bytes 0-0/1".into()}else{format!("bytes {start}-{end}/{FILE_SIZE}")}).body(Full::new(Bytes::from(vec![b'x';length]))).unwrap())
                        }
                    })).await;
                });
            }
        })
    };
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
    let id = create_sandbox(api.as_ref()).await;
    let route:crate::common::route::RouteInfo=serde_json::from_value(value!({"instanceID":id,"instanceStatus":{"code":3},"tenantID":"tenant","sandboxID":"runtime","sandboxIP":"127.0.0.1","nodeProxyAddress":address.to_string()})).unwrap();
    node.activate_route(
        route.instance_id.clone(),
        route.sandbox_id.clone(),
        route.sandbox_ip.parse().unwrap(),
    )
    .await;
    routes.put(route.clone());
    let reconcile = tokio::spawn(gateway.clone().run_route_reconciler(routes.subscribe()));
    let download = |token: String| {
        Download {
        ingress:gateway.clone(),context:Arc::new(RequestContext::new(Duration::from_secs(10))),tenant:"tenant".into(),sandbox_id:id.clone(),
        config:DownloadConfig::from_template(&serde_json::from_value(value!({"name":"jiuwen","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"env":{"JIUWENSWARM_WORKSPACE":"/work","JIUWENSWARM_DOWNLOAD_ASSET_ROOT":"/assets","JIUWENSWARM_FILE_DOWNLOAD_SECRET":"s".repeat(32)},"service":[{"protocol":"ws","port":18091}]})).unwrap()).unwrap(),
        token,expected_session:Some("session-a".into()),
    }
    };
    let mut headers = http::HeaderMap::new();
    headers.insert("range", "bytes=1-65538".parse().unwrap());
    let response = download(token(4102444800.0, true))
        .respond(http::Method::GET, &headers, true)
        .await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response.headers()["content-range"],
        format!("bytes 1-65538/{FILE_SIZE}")
    );
    assert_eq!(response.collect().await.unwrap().to_bytes().len(), 65538);
    assert_eq!(
        backend.queries.load(Ordering::SeqCst),
        1,
        "one directory query for all download chunks"
    );
    assert_eq!(
        *ranges.lock().unwrap(),
        ["bytes=1-65536", "bytes=65537-65538"]
    );
    ranges.lock().unwrap().clear();
    let response = download(token(4102444800.0, true))
        .respond(http::Method::HEAD, &headers, false)
        .await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert!(response.collect().await.unwrap().to_bytes().is_empty());
    assert_eq!(
        backend.queries.load(Ordering::SeqCst),
        2,
        "a new HTTP request needs fresh admission"
    );
    assert!(ranges.lock().unwrap().is_empty());
    headers.insert("range", "bytes=999999-".parse().unwrap());
    let response = download(token(4102444800.0, true))
        .respond(http::Method::GET, &headers, false)
        .await;
    assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(
        response.headers()["content-range"],
        format!("bytes */{FILE_SIZE}")
    );
    assert!(ranges.lock().unwrap().is_empty());
    // Ordinary attachments keep returning a full body even with Range and inline.
    let response = download(token(0.0, false))
        .respond(http::Method::GET, &headers, true)
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!response.headers().contains_key("content-range"));
    assert!(response.headers()["content-disposition"]
        .to_str()
        .unwrap()
        .starts_with("attachment;"));
    assert_eq!(
        response.collect().await.unwrap().to_bytes().len(),
        FILE_SIZE
    );
    ranges.lock().unwrap().clear();
    headers.clear();
    let response = download(token(4102444800.0, true))
        .respond(http::Method::GET, &headers, false)
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    assert_eq!(
        body.frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap()
            .len(),
        65536
    );
    active.store(false, Ordering::SeqCst);
    assert!(body.frame().await.unwrap().is_err());
    assert_eq!(ranges.lock().unwrap().len(), 1);
    active.store(true, Ordering::SeqCst);
    ranges.lock().unwrap().clear();
    // Buffered first bytes must not bypass revocation while the consumer is idle.
    let response = download(token(4102444800.0, true))
        .respond(http::Method::GET, &headers, false)
        .await;
    active.store(false, Ordering::SeqCst);
    assert!(response.into_body().frame().await.unwrap().is_err());
    active.store(true, Ordering::SeqCst);
    ranges.lock().unwrap().clear();
    // The original request deadline includes downstream backpressure.
    let mut delayed = download(token(4102444800.0, true));
    delayed.context = Arc::new(RequestContext::new(Duration::from_millis(300)));
    let response = delayed.respond(http::Method::GET, &headers, false).await;
    assert_eq!(response.status(), StatusCode::OK);
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(response.into_body().frame().await.unwrap().is_err());
    ranges.lock().unwrap().clear();
    for fault in 1..=4 {
        response_fault.store(fault, Ordering::SeqCst);
        let response = download(token(4102444800.0, true))
            .respond(http::Method::GET, &headers, false)
            .await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY, "fault {fault}");
        let bytes = response.collect().await.unwrap().to_bytes();
        assert!(!String::from_utf8_lossy(&bytes).contains("runtime-only-secret"));
    }
    response_fault.store(0, Ordering::SeqCst);
    let response = download("bad.token".into())
        .respond(http::Method::GET, &headers, false)
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let mut invalid_signature = token(4102444800.0, true);
    let last = invalid_signature.pop().unwrap();
    invalid_signature.push(if last == '0' { '1' } else { '0' });
    let response = download(invalid_signature)
        .respond(http::Method::GET, &headers, false)
        .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let error: Value =
        serde_json::from_slice(&response.collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(error["code"], "FORBIDDEN");
    let response = download(token(4102444800.0, true))
        .respond(http::Method::POST, &headers, false)
        .await;
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    ranges.lock().unwrap().clear();
    // A route revoked between chunks stays revoked even if the same route is restored.
    let before = backend.queries.load(Ordering::SeqCst);
    let response = download(token(4102444800.0, true))
        .respond(http::Method::GET, &headers, false)
        .await;
    let mut body = response.into_body();
    assert_eq!(
        body.frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap()
            .len(),
        65536
    );
    routes.delete(&id);
    routes.put(route.clone());
    assert!(body.frame().await.unwrap().is_err());
    assert_eq!(backend.queries.load(Ordering::SeqCst) - before, 1);
    assert_eq!(
        ranges.lock().unwrap().len(),
        1,
        "no read after route revocation"
    );
    ranges.lock().unwrap().clear();
    assert_eq!(gateway.active_sessions(), 0);
    reconcile.abort();
    node_task.abort();
    runtime_task.abort();
}
