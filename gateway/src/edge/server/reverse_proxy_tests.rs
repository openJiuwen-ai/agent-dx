use super::*;
use crate::edge::{H2PoolConfig, RouteStore};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Notify;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{sleep, timeout};

type Sender = hyper::client::conn::http1::SendRequest<Full<Bytes>>;

struct Fixture {
    address: SocketAddr,
    accepted: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    requests: Arc<AtomicUsize>,
    release: Arc<Notify>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

struct ActiveConnection(Arc<AtomicUsize>);
impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn backend_connection(
    mut stream: TcpStream,
    requests: Arc<AtomicUsize>,
    release: Arc<Notify>,
) {
    loop {
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            if stream.read_exact(&mut byte).await.is_err() {
                return;
            }
            head.push(byte[0]);
        }
        let head = String::from_utf8(head).unwrap();
        requests.fetch_add(1, Ordering::SeqCst);
        let content_length = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        let mut body = vec![0; content_length];
        if stream.read_exact(&mut body).await.is_err() {
            return;
        }
        let path = head.split_whitespace().nth(1).unwrap();
        match path {
            "/login" => {
                if stream.write_all(b"HTTP/1.1 302 Found\r\nLocation: /grafana/login?next=%2Fgrafana%2F\r\nSet-Cookie: session=one; Path=/grafana/; HttpOnly\r\nSet-Cookie: csrf=two; Path=/grafana/\r\nContent-Length: 0\r\n\r\n").await.is_err() { return; }
            }
            "/api/fail" => return,
            "/api/stream" => {
                if stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n3\r\none\r\n").await.is_err() { return; }
                let mut byte = [0];
                tokio::select! {
                    _ = release.notified() => {},
                    _ = stream.read(&mut byte) => return,
                }
                if stream.write_all(b"3\r\ntwo\r\n0\r\n\r\n").await.is_err() {
                    return;
                }
            }
            "/api/upgrade" => {
                if stream.write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n").await.is_err() { return; }
                let (mut reader, mut writer) = stream.split();
                let _ = tokio::io::copy(&mut reader, &mut writer).await;
                return;
            }
            _ => {
                let close = path == "/api/close";
                let mut reply = head.into_bytes();
                reply.extend(body);
                let response = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nSet-Cookie: a=1\r\nSet-Cookie: b=2\r\n{}\r\n", reply.len(), if close { "Connection: close\r\n" } else { "" });
                if stream.write_all(response.as_bytes()).await.is_err()
                    || stream.write_all(&reply).await.is_err()
                {
                    return;
                }
                if close {
                    return;
                }
            }
        }
    }
}

impl Fixture {
    async fn new(config: ReverseProxyConfig) -> Self {
        Self::with_mount(config, None).await
    }

    async fn with_mount(config: ReverseProxyConfig, mount: Option<&str>) -> Self {
        let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_address = backend.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let backend_task = {
            let (accepted, active, requests, release) = (
                accepted.clone(),
                active.clone(),
                requests.clone(),
                release.clone(),
            );
            tokio::spawn(async move {
                let mut tasks = JoinSet::new();
                loop {
                    tokio::select! {
                        result = backend.accept() => {
                            let (stream, _) = result.unwrap();
                            accepted.fetch_add(1, Ordering::SeqCst);
                            active.fetch_add(1, Ordering::SeqCst);
                            let guard = ActiveConnection(active.clone());
                            let (requests, release) = (requests.clone(), release.clone());
                            tasks.spawn(async move {
                                let _guard = guard;
                                backend_connection(stream, requests, release).await;
                            });
                        }
                        _ = tasks.join_next(), if !tasks.is_empty() => {},
                    }
                }
            })
        };
        let edge = Arc::new(
            EdgeFrontend::new(
                Arc::new(EdgeRouteResolver::new(Arc::new(RouteStore::new()))),
                DataPlaneL4Connector::new(H2PoolConfig::default()),
                EdgeAuthenticator::new(true, false, "", Duration::from_secs(30)).unwrap(),
                8888,
                8765,
                backend_address.to_string(),
                crate::edge::parse_static_routes("prefix:/api").unwrap(),
            )
            .with_reverse_proxy_config(config)
            .with_proxy_routes(
                mount
                    .map(|prefix| {
                        vec![ProxyRoute {
                            name: "grafana".into(),
                            path_prefix: prefix.into(),
                            upstream: format!("http://{backend_address}"),
                            strip_prefix: true,
                            host: None,
                        }]
                    })
                    .unwrap_or_default(),
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let edge_task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        let (stream, peer) = result.unwrap();
                        let edge = edge.clone();
                        tasks.spawn(async move {
                            let service = service_fn(move |request| {
                                let edge = edge.clone();
                                async move { edge.handle_http(request, IngressSecurity::Tls, peer).await }
                            });
                            let _ = hyper::server::conn::http1::Builder::new()
                                .serve_connection(TokioIo::new(stream), service).with_upgrades().await;
                        });
                    }
                    _ = tasks.join_next(), if !tasks.is_empty() => {},
                }
            }
        });
        Self {
            address,
            accepted,
            active,
            requests,
            release,
            tasks: vec![backend_task, edge_task],
        }
    }

    async fn sender(&mut self) -> Sender {
        let stream = TcpStream::connect(self.address).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        self.tasks.push(tokio::spawn(async move {
            let _ = connection.with_upgrades().await;
        }));
        sender
    }
}

fn request(path: &str) -> Request<Full<Bytes>> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header("host", "public.example:8443")
        .header("x-request-id", "stable-id")
        .header("content-length", "5")
        .body(Full::new(Bytes::from_static(b"hello")))
        .unwrap()
}

async fn complete(sender: &mut Sender, path: &str) -> Bytes {
    let response = timeout(Duration::from_secs(3), sender.send_request(request(path)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get_all("set-cookie").iter().count(), 2);
    response.into_body().collect().await.unwrap().to_bytes()
}

async fn wait_active(fixture: &Fixture, expected: usize) {
    timeout(Duration::from_secs(3), async {
        while fixture.active.load(Ordering::SeqCst) != expected {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn reverse_proxy_reuses_connections_and_preserves_request() {
    for idle_limit in [0, 4] {
        let mut fixture = Fixture::new(ReverseProxyConfig {
            max_idle_connections: idle_limit,
            ..Default::default()
        })
        .await;
        let mut sender = fixture.sender().await;
        let start = Instant::now();
        for _ in 0..100 {
            let response = complete(&mut sender, "/api/echo?q=a%2Fb").await;
            let response = String::from_utf8(response.to_vec()).unwrap().to_lowercase();
            for value in [
                "post /api/echo?q=a%2fb http/1.1",
                "host: public.example:8443",
                "x-forwarded-host: public.example:8443",
                "x-forwarded-proto: https",
                "x-real-ip: 127.0.0.1",
                "x-request-id: stable-id",
            ] {
                assert!(response.contains(value), "missing {value}: {response}");
            }
            assert!(response.ends_with("hello"));
        }
        let connections = fixture.accepted.load(Ordering::SeqCst);
        println!("frontend keep-alive: idle_limit={idle_limit}, requests=100, connections={connections}, elapsed_ms={}", start.elapsed().as_millis());
        assert_eq!(connections, if idle_limit == 0 { 100 } else { 1 });
    }
}

#[tokio::test]
async fn reverse_proxy_stream_does_not_block_other_requests() {
    let mut fixture = Fixture::new(ReverseProxyConfig {
        max_idle_connections: 1,
        ..Default::default()
    })
    .await;
    let mut streaming = fixture.sender().await;
    let mut body = streaming
        .send_request(request("/api/stream"))
        .await
        .unwrap()
        .into_body();
    let first = timeout(Duration::from_secs(3), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert_eq!(first, "one");
    let mut other = fixture.sender().await;
    complete(&mut other, "/api/normal").await;
    assert_eq!(fixture.accepted.load(Ordering::SeqCst), 2);
    fixture.release.notify_one();
    assert_eq!(body.collect().await.unwrap().to_bytes(), "two");
    wait_active(&fixture, 1).await;
    complete(&mut other, "/api/normal").await;
    assert_eq!(fixture.accepted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn reverse_proxy_incomplete_response_is_discarded() {
    let mut fixture = Fixture::new(ReverseProxyConfig::default()).await;
    let mut sender = fixture.sender().await;
    let mut body = sender
        .send_request(request("/api/stream"))
        .await
        .unwrap()
        .into_body();
    body.frame().await.unwrap().unwrap();
    drop(body);
    drop(sender);
    wait_active(&fixture, 0).await;
    let mut next = fixture.sender().await;
    complete(&mut next, "/api/normal").await;
    assert_eq!(fixture.accepted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn reverse_proxy_idle_connections_expire() {
    let mut fixture = Fixture::new(ReverseProxyConfig {
        idle_timeout: Duration::from_millis(50),
        ..Default::default()
    })
    .await;
    let mut sender = fixture.sender().await;
    complete(&mut sender, "/api/normal").await;
    wait_active(&fixture, 0).await;
    complete(&mut sender, "/api/normal").await;
    assert_eq!(fixture.accepted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn reverse_proxy_post_failure_is_not_replayed() {
    let mut fixture = Fixture::new(ReverseProxyConfig::default()).await;
    let mut sender = fixture.sender().await;
    complete(&mut sender, "/api/normal").await;
    let response = sender.send_request(request("/api/fail")).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    response.into_body().collect().await.unwrap();
    assert_eq!(fixture.requests.load(Ordering::SeqCst), 2);
    complete(&mut sender, "/api/normal").await;
    assert_eq!(fixture.accepted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn reverse_proxy_connection_close_is_not_reused() {
    let mut fixture = Fixture::new(ReverseProxyConfig::default()).await;
    let mut sender = fixture.sender().await;
    complete(&mut sender, "/api/close").await;
    let mut sender = fixture.sender().await;
    complete(&mut sender, "/api/normal").await;
    assert_eq!(fixture.accepted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn reverse_proxy_upgrade_keeps_its_own_connection() {
    let mut fixture = Fixture::with_mount(ReverseProxyConfig::default(), Some("/grafana")).await;
    let mut sender = fixture.sender().await;
    let mut req = request("/grafana/api/upgrade");
    req.headers_mut().insert(
        header::CONNECTION,
        http::HeaderValue::from_static("upgrade"),
    );
    req.headers_mut()
        .insert(header::UPGRADE, http::HeaderValue::from_static("websocket"));
    let response = sender.send_request(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    let mut upgraded = TokioIo::new(hyper::upgrade::on(response).await.unwrap());
    upgraded.write_all(b"ping").await.unwrap();
    let mut echoed = [0; 4];
    timeout(Duration::from_secs(3), upgraded.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&echoed, b"ping");
    let mut other = fixture.sender().await;
    for _ in 0..3 {
        complete(&mut other, "/api/normal").await;
    }
    assert_eq!(fixture.accepted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn reverse_proxy_configured_prefix_rewrites_path_and_keeps_frontend_routes() {
    let mut fixture = Fixture::with_mount(ReverseProxyConfig::default(), Some("/grafana")).await;
    let mut sender = fixture.sender().await;
    let response = complete(&mut sender, "/grafana/api/echo?q=a%2Fb").await;
    let text = String::from_utf8(response.to_vec()).unwrap().to_lowercase();
    assert!(text.contains("post /api/echo?q=a%2fb http/1.1"));
    assert!(text.contains("x-forwarded-prefix: /grafana"));
    assert!(text.contains("host: public.example:8443"));
    let direct = complete(&mut sender, "/api/normal").await;
    assert!(!String::from_utf8(direct.to_vec())
        .unwrap()
        .contains("x-forwarded-prefix"));
    let boundary = sender
        .send_request(request("/grafana2/api/echo"))
        .await
        .unwrap();
    assert_ne!(boundary.status(), StatusCode::OK);
}

#[tokio::test]
async fn reverse_proxy_preserves_app_redirects_and_cookie_paths() {
    let mut fixture = Fixture::with_mount(ReverseProxyConfig::default(), Some("/grafana")).await;
    let mut sender = fixture.sender().await;
    let response = sender
        .send_request(request("/grafana/login"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FOUND);
    assert_eq!(
        response.headers()[header::LOCATION],
        "/grafana/login?next=%2Fgrafana%2F"
    );
    let cookies: Vec<_> = response
        .headers()
        .get_all(header::SET_COOKIE)
        .iter()
        .collect();
    assert_eq!(cookies.len(), 2);
    assert!(cookies
        .iter()
        .all(|value| value.to_str().unwrap().contains("Path=/grafana/")));
    response.into_body().collect().await.unwrap();
}

#[tokio::test]
async fn reverse_proxy_removes_connection_nominated_headers() {
    let mut fixture = Fixture::new(ReverseProxyConfig::default()).await;
    let mut sender = fixture.sender().await;
    let mut req = request("/api/normal");
    req.headers_mut().insert(
        header::CONNECTION,
        http::HeaderValue::from_static("x-hop-secret, keep-alive"),
    );
    req.headers_mut()
        .insert("x-hop-secret", http::HeaderValue::from_static("private"));
    req.headers_mut()
        .insert("forwarded", http::HeaderValue::from_static("for=attacker"));
    let response = sender.send_request(req).await.unwrap();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap().to_lowercase();
    assert!(!text.contains("x-hop-secret"));
    assert!(!text.contains("for=attacker"));
}
