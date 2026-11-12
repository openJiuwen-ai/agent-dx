//! M0-1 出口统一（详设 §6.2「M0-1 实现方案」）：请求族必须覆盖早 400，
//! 内层路径恰好计一次，且既有族带 `# TYPE` 声明。
//! M0-2 全流族（§6.2「M0-2 实现方案」）：HTTP body 生命周期样本与稠密导出。
//! 黑盒验证：data plane 端口发请求，health 端口读 `/metrics` 断言。
use data_plane_gateway::common::protocol::{ConnectTarget, GatewayPolicy};
use data_plane_gateway::common::route::{EnvironmentStatus, RouteInfo};
use data_plane_gateway::ingress::auth::{AuthError, AuthenticatedIdentity, CredentialVerifier};
use data_plane_gateway::ingress::resolver::DemandResolver;
use data_plane_gateway::ingress::{
    AccessKind, DataPlaneL4Connector, H2PoolConfig, Ingress, IngressAuthenticator,
    IngressRouteResolver, ResolveError, RouteHandle, RouteStore,
};
use data_plane_gateway::node::Relay;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::watch;

#[derive(Debug)]
struct Identity;
#[async_trait::async_trait]
impl CredentialVerifier for Identity {
    async fn verify(&self, key: &str) -> Result<AuthenticatedIdentity, AuthError> {
        match key {
            "tenant" => Ok(AuthenticatedIdentity {
                tenant_id: key.into(),
                expires_at_unix: None,
            }),
            _ => Err(AuthError::Invalid("test credential".into())),
        }
    }
}

struct Fixture {
    data_plane: std::net::SocketAddr,
    health: std::net::SocketAddr,
    store: Arc<RouteStore>,
    _shutdown: watch::Sender<bool>,
}

async fn fixture() -> Fixture {
    fixture_with_full_stream(true).await
}

async fn fixture_with_full_stream(full_stream: bool) -> Fixture {
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
        .with_client_acl(Vec::new(), true)
        .with_full_stream_observation(full_stream),
    );
    let (shutdown, receiver) = watch::channel(false);
    let data_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let health_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let data_plane = data_listener.local_addr().unwrap();
    let health = health_listener.local_addr().unwrap();
    tokio::spawn(gateway.clone().serve_http(data_listener, receiver.clone()));
    tokio::spawn(gateway.clone().serve_health(health_listener, receiver));
    Fixture {
        data_plane,
        health,
        store,
        _shutdown: shutdown,
    }
}

/// 可控上游（详设 M0-3 测试契约）：accept 后保持连接挂起（不读不写）。
async fn hanging_upstream() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });
    address
}

/// 本机非回环地址（UDP connect 只选路由、不发包）——GatewayPolicy::new 把回环当
/// 保留目标拒绝（403），relay 后端必须绑非回环地址。
fn local_ip() -> std::net::IpAddr {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").expect("bind udp socket");
    socket.connect("8.8.8.8:80").expect("select a route");
    socket.local_addr().expect("local address").ip()
}

/// SSE 上游：立即返回 200＋text/event-stream 头，之后不发任何 body。
async fn hanging_sse_upstream() -> std::net::SocketAddr {
    let listener = TcpListener::bind((local_ip(), 0)).await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                use tokio::io::AsyncWriteExt;
                let _ = stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\
Transfer-Encoding: chunked\r\n\r\n",
                    )
                    .await;
                // 不发 terminating chunk：body 挂起，等待 DeadlineBody 截断。
                std::future::pending::<()>().await;
            });
        }
    });
    address
}

fn put_route(store: &RouteStore, instance: &str, upstream: std::net::SocketAddr) {
    let route: data_plane_gateway::common::route::RouteInfo =
        serde_json::from_value(serde_json::json!({
            "instanceID": instance,
            "instanceStatus": {"code": 3},
            "tenantID": "tenant",
            "environmentID": "runtime-test",
            "environmentIP": "127.0.0.1",
            "nodeProxyAddress": upstream.to_string(),
        }))
        .unwrap();
    store.put(route);
}

/// 隧道后端：accept → 写 ready → 关写侧 → 排空读侧（双向干净 EOF）。
async fn tunnel_backend() -> std::net::SocketAddr {
    let listener = TcpListener::bind((local_ip(), 0)).await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let _ = stream.write_all(b"ready").await;
                let _ = stream.shutdown().await;
                let mut sink = [0u8; 1024];
                loop {
                    match stream.read(&mut sink).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
            });
        }
    });
    address
}

/// WS 后端：读走升级请求 → 回 101 → 保持连接直到对端关闭。
async fn ws_backend() -> std::net::SocketAddr {
    let listener = TcpListener::bind((local_ip(), 0)).await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                loop {
                    match stream.read(&mut byte).await {
                        Ok(0) | Err(_) => return,
                        Ok(_) => {
                            head.push(byte[0]);
                            if head.ends_with(b"\r\n\r\n") {
                                break;
                            }
                        }
                    }
                }
                let _ = stream
                    .write_all(
                        b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\n\
Upgrade: websocket\r\n\r\n",
                    )
                    .await;
                let mut sink = [0u8; 1024];
                loop {
                    match stream.read(&mut sink).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
            });
        }
    });
    address
}

/// 永不完成的唤醒：needs_resume 路由＋停滞 ensure_running → 预算耗尽 → Resuming。
struct StalledDemand;
#[async_trait::async_trait]
impl DemandResolver for StalledDemand {
    async fn describe(
        &self,
        id: &str,
        port: u16,
        kind: AccessKind,
        request_id: String,
    ) -> Result<RouteHandle, ResolveError> {
        Ok(RouteHandle {
            private_service_ports: Vec::new(),
            e2b: None,
            needs_resume: true,
            processing: None,
            access_kind: kind,
            relay_address: "127.0.0.1:1".into(),
            tenant_id: String::new(),
            tunnel_security_mode: Default::default(),
            port_forward_auth_mode: Default::default(),
            target: ConnectTarget {
                instance_id: id.into(),
                workload_id: "workload".into(),
                target_ip: "127.0.0.1".parse().unwrap(),
                target_port: port,
                request_id,
            },
        })
    }
    async fn ensure_running(&self, _: &RouteHandle) -> Result<String, ResolveError> {
        std::future::pending().await
    }
}

/// demand 夹具：stream_only＋停滞唤醒——store 未命中走 demand，唤醒永不完成。
async fn demand_fixture() -> Fixture {
    let store = Arc::new(RouteStore::new());
    store.set_ready(true);
    let gateway = Arc::new(
        Ingress::new(
            Arc::new(
                IngressRouteResolver::new(store.clone())
                    .stream_only()
                    .with_demand_resolver(Arc::new(StalledDemand)),
            ),
            DataPlaneL4Connector::new(H2PoolConfig::default()),
            IngressAuthenticator::with_verifier(Arc::new(Identity)),
            50090,
            8765,
            "127.0.0.1:1",
            vec![],
        )
        .with_client_acl(Vec::new(), true),
    );
    let (shutdown, receiver) = watch::channel(false);
    let data_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let health_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let data_plane = data_listener.local_addr().unwrap();
    let health = health_listener.local_addr().unwrap();
    tokio::spawn(gateway.clone().serve_http(data_listener, receiver.clone()));
    tokio::spawn(gateway.clone().serve_health(health_listener, receiver));
    Fixture {
        data_plane,
        health,
        store,
        _shutdown: shutdown,
    }
}

/// relay 夹具（mock_e2e 模式）：真 Relay 节点桥接可控后端，路由注册为 instance-x。
async fn relay_fixture(backend: std::net::SocketAddr) -> Fixture {
    let node = Arc::new(Relay::new(GatewayPolicy::new(vec![format!(
        "{}/32",
        backend.ip()
    )
    .parse()
    .unwrap()])));
    let node_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_address = node_listener.local_addr().unwrap();
    let relay_node = Arc::clone(&node);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = node_listener.accept().await else {
                break;
            };
            let node = Arc::clone(&relay_node);
            tokio::spawn(async move { node.serve_h2(stream).await });
        }
    });
    node.activate_route("instance-x".into(), "instance-x".into(), backend.ip())
        .await;
    let store = Arc::new(RouteStore::new());
    store.set_ready(true);
    store.put(RouteInfo {
        private_service_ports: Vec::new(),
        instance_id: "instance-x".into(),
        environment_status: EnvironmentStatus {
            code: 3,
            ..Default::default()
        },
        environment_id: "instance-x".into(),
        environment_ip: backend.ip().to_string(),
        relay_address: relay_address.to_string(),
        tenant_id: String::new(),
        tunnel_security_mode: Default::default(),
        port_forward_security_mode: Default::default(),
        port_forward_routes: Vec::new(),
        e2b: None,
    });
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
        .with_client_acl(Vec::new(), true),
    );
    let (shutdown, receiver) = watch::channel(false);
    let data_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let health_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let data_plane = data_listener.local_addr().unwrap();
    let health = health_listener.local_addr().unwrap();
    tokio::spawn(gateway.clone().serve_http(data_listener, receiver.clone()));
    tokio::spawn(gateway.clone().serve_health(health_listener, receiver));
    Fixture {
        data_plane,
        health,
        store,
        _shutdown: shutdown,
    }
}

async fn send(
    data_plane: std::net::SocketAddr,
    request: Request<Full<Bytes>>,
) -> hyper::Response<hyper::body::Incoming> {
    let stream = tokio::net::TcpStream::connect(data_plane).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(connection);
    sender.send_request(request).await.unwrap()
}

async fn metrics_text(health: std::net::SocketAddr) -> String {
    let stream = tokio::net::TcpStream::connect(health).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(connection);
    let response = sender
        .send_request(
            Request::get("/metrics")
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// T1：早 400（非法超时头）也计入请求族——入口拒绝不再漏计。
#[tokio::test]
async fn early_bad_request_is_counted() {
    let fixture = fixture().await;
    let response = send(
        fixture.data_plane,
        Request::get("/anything")
            .header("x-adx-processing-timeout-ms", "abc")
            .body(Full::new(Bytes::new()))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let text = metrics_text(fixture.health).await;
    assert!(
        text.contains("data_plane_ingress_http_requests_total 1\n"),
        "requests_total 漏计早 400：\n{text}"
    );
    assert!(
        text.contains("data_plane_ingress_http_responses_total{class=\"4xx\"} 1\n"),
        "4xx 计数漏计早 400：\n{text}"
    );
}

/// T1 变体：重复超时头同样在入口被拒、同样计数。
#[tokio::test]
async fn duplicate_timeout_header_is_counted() {
    let fixture = fixture().await;
    let response = send(
        fixture.data_plane,
        Request::get("/anything")
            .header("x-adx-processing-timeout-ms", "10")
            .header("x-adx-processing-timeout-ms", "20")
            .body(Full::new(Bytes::new()))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let text = metrics_text(fixture.health).await;
    assert!(
        text.contains("data_plane_ingress_http_requests_total 1\n"),
        "requests_total 漏计重复头早 400：\n{text}"
    );
    assert!(
        text.contains("data_plane_ingress_http_responses_total{class=\"4xx\"} 1\n"),
        "4xx 计数漏计重复头早 400：\n{text}"
    );
}

/// T2：经内层的请求恰好计一次（双计守卫：观测点在外层，不得重复）。
#[tokio::test]
async fn inner_response_counted_exactly_once() {
    let fixture = fixture().await;
    let response = send(
        fixture.data_plane,
        Request::get("/unmatched-path")
            .body(Full::new(Bytes::new()))
            .unwrap(),
    )
    .await;
    let status = response.status();
    let text = metrics_text(fixture.health).await;
    let class = status.as_u16() / 100;
    assert!(
        text.contains("data_plane_ingress_http_requests_total 1\n"),
        "requests_total 应恰好 +1：\n{text}"
    );
    assert!(
        text.contains(&format!(
            "data_plane_ingress_http_responses_total{{class=\"{class}xx\"}} 1\n"
        )),
        "状态 {status} 应计入对应类别：\n{text}"
    );
    assert!(
        !text.contains("data_plane_ingress_http_requests_total 2"),
        "双计守卫：requests_total 不得为 2：\n{text}"
    );
}

/// M0：请求族补齐 `# TYPE` 声明（直方图族声明一次、位于桶行之前）。
#[tokio::test]
async fn request_family_declares_types() {
    let fixture = fixture().await;
    let text = metrics_text(fixture.health).await;
    for line in [
        "# TYPE data_plane_ingress_http_requests_in_flight gauge\n",
        "# TYPE data_plane_ingress_http_requests_total counter\n",
        "# TYPE data_plane_ingress_http_responses_total counter\n",
        "# TYPE data_plane_ingress_http_request_duration_seconds histogram\n",
    ] {
        assert!(text.contains(line), "缺少 TYPE 声明 {line:?}：\n{text}");
    }
    let histogram = text
        .find("# TYPE data_plane_ingress_http_request_duration_seconds histogram\n")
        .expect("histogram TYPE 声明存在");
    let first_bucket = text
        .find("data_plane_ingress_http_request_duration_seconds_bucket{")
        .expect("bucket 样本存在");
    assert!(
        histogram < first_bucket,
        "TYPE 声明必须位于首个 bucket 行之前：\n{text}"
    );
}

/// F1：全流样本——早 400 与内层 404 都以 unmatched/http/success 各计一次。
#[tokio::test]
async fn full_stream_samples_record_request_lifecycle() {
    let fixture = fixture().await;
    let response = send(
        fixture.data_plane,
        Request::get("/anything")
            .header("x-adx-processing-timeout-ms", "abc")
            .body(Full::new(Bytes::new()))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    // 读尽 body，确保服务侧流结束（success）已被记录。
    let _ = response.into_body().collect().await;

    let response = send(
        fixture.data_plane,
        Request::get("/unmatched-path")
            .body(Full::new(Bytes::new()))
            .unwrap(),
    )
    .await;
    let _ = response.into_body().collect().await;

    let text = metrics_text(fixture.health).await;
    assert!(
        text.contains(
            "data_plane_ingress_full_stream_duration_seconds_count{route_class=\"unmatched\",\
transport=\"http\",outcome=\"success\"} 2\n"
        ),
        "早 400 与内层 404 应各计一次全流样本：\n{text}"
    );
}

/// F2：稠密导出——80 个直方图组合与 20 个中断组合全部出现（含全零）。
#[tokio::test]
async fn full_stream_families_export_dense_zero_combinations() {
    let fixture = fixture().await;
    let text = metrics_text(fixture.health).await;
    assert_eq!(
        text.matches("data_plane_ingress_full_stream_duration_seconds_count{")
            .count(),
        80,
        "直方图组合数应为 5×4×4：\n{text}"
    );
    assert_eq!(
        text.matches("data_plane_ingress_stream_aborted_total{")
            .count(),
        20,
        "中断组合数应为 5×4：\n{text}"
    );
    assert!(
        text.contains(
            "data_plane_ingress_full_stream_duration_seconds_count{route_class=\"jiuwen\",\
transport=\"ws\",outcome=\"unknown\"} 0\n"
        ),
        "全零组合必须稠密导出：\n{text}"
    );
    assert!(
        text.contains(
            "data_plane_ingress_stream_aborted_total{route_class=\"environment_api\",\
reason=\"client_closed\"} 0\n"
        ),
        "{text}"
    );
    assert!(text.contains("# TYPE data_plane_ingress_full_stream_duration_seconds histogram\n",));
    assert!(text.contains("# TYPE data_plane_ingress_stream_aborted_total counter\n",));
    assert!(text.contains("# HELP data_plane_ingress_full_stream_duration_seconds "));
    assert!(text.contains("# HELP data_plane_ingress_stream_aborted_total "));
}

/// F3：开关关闭后不导出全流族（压测对比开销，M-A15）。
#[tokio::test]
async fn full_stream_observation_can_be_disabled() {
    let fixture = fixture_with_full_stream(false).await;
    let response = send(
        fixture.data_plane,
        Request::get("/unmatched-path")
            .body(Full::new(Bytes::new()))
            .unwrap(),
    )
    .await;
    let _ = response.into_body().collect().await;
    let text = metrics_text(fixture.health).await;
    assert!(
        !text.contains("data_plane_ingress_full_stream"),
        "关闭后不应出现全流族：\n{text}"
    );
    assert!(
        !text.contains("data_plane_ingress_stream_aborted_total"),
        "{text}"
    );
    // 请求族不受开关影响。
    assert!(text.contains("data_plane_ingress_http_requests_total 1\n"));
}

/// T3：外层超时 504 计入请求族与全流（挂起上游＋1ms 预算；M0-1 遗留测试债）。
#[tokio::test]
async fn outer_processing_timeout_is_counted() {
    let fixture = fixture().await;
    put_route(&fixture.store, "instance-a", hanging_upstream().await);
    let response = send(
        fixture.data_plane,
        Request::get("/instance-a/8080/x")
            .header("x-adx-processing-timeout-ms", "1")
            .body(Full::new(Bytes::new()))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    let _ = response.into_body().collect().await;
    let text = metrics_text(fixture.health).await;
    assert!(
        text.contains("data_plane_ingress_http_responses_total{class=\"5xx\"} 1\n"),
        "{text}"
    );
    assert!(
        text.contains(
            "data_plane_ingress_full_stream_duration_seconds_count{route_class=\"environment_api\",\
transport=\"http\",outcome=\"success\"} 1\n"
        ),
        "504 body 完整送达——传输层结论与业务状态分离：\n{text}"
    );
}

/// T4：ENV_RESUMING 重写后的终态计入（needs_resume → 唤醒耗尽预算 → Resuming；
/// M0-1 遗留测试债。NotReady 是裸 503 不带该头，见 `error_response`）。
#[tokio::test]
async fn resuming_rewrite_observes_final_status() {
    let fixture = demand_fixture().await;
    let response = tokio::time::timeout(
        std::time::Duration::from_millis(600),
        send(
            fixture.data_plane,
            Request::get("/instance-e/8080/x")
                .header("x-adx-processing-timeout-ms", "120")
                .body(Full::new(Bytes::new()))
                .unwrap(),
        ),
    )
    .await
    .expect("唤醒等待必须在预算内返回 ENV_RESUMING，而不是挂起");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let error_code = response
        .headers()
        .get("x-adx-error-code")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body = String::from_utf8_lossy(&body).into_owned();
    assert_eq!(
        error_code.as_deref(),
        Some("ENV_RESUMING"),
        "ENV_RESUMING 头缺失，实际响应体：\n{body}"
    );
    let text = metrics_text(fixture.health).await;
    assert!(
        text.contains("data_plane_ingress_http_responses_total{class=\"5xx\"} 1\n"),
        "{text}"
    );
    assert!(
        text.contains(
            "data_plane_ingress_full_stream_duration_seconds_count{route_class=\"environment_api\",\
transport=\"http\",outcome=\"success\"} 1\n"
        ),
        "{text}"
    );
}

/// T5：SSE 分类＋deadline 截断（挂起 SSE 后端＋预算 → DeadlineBody 截断为 failure）。
#[tokio::test]
async fn sse_stream_deadline_truncation_is_counted() {
    let backend = hanging_sse_upstream().await;
    let fixture = relay_fixture(backend).await;
    let response = tokio::time::timeout(
        std::time::Duration::from_millis(600),
        send(
            fixture.data_plane,
            Request::get(format!("/instance-x/{}/x", backend.port()))
                .header("x-adx-processing-timeout-ms", "150")
                .body(Full::new(Bytes::new()))
                .unwrap(),
        ),
    )
    .await
    .expect("响应头必须在预算内返回");
    assert_eq!(response.status(), StatusCode::OK);
    // body 被 DeadlineBody 截断：客户端读到错误结束的流（而不是永远挂起）。
    let collected = tokio::time::timeout(
        std::time::Duration::from_millis(600),
        response.into_body().collect(),
    )
    .await
    .expect("body 必须被 DeadlineBody 截断，而不是永远挂起");
    assert!(collected.is_err(), "截断的 body 应以错误结束");
    let text = metrics_text(fixture.health).await;
    assert!(
        text.contains("data_plane_ingress_http_responses_total{class=\"2xx\"} 1\n"),
        "{text}"
    );
    assert!(
        text.contains(
            "data_plane_ingress_full_stream_duration_seconds_count{route_class=\"environment_api\",\
transport=\"sse\",outcome=\"failure\"} 1\n"
        ),
        "Content-Type: text/event-stream 应记 transport=sse 且截断为 failure：\n{text}"
    );
    assert!(
        text.contains(
            "data_plane_ingress_stream_aborted_total{route_class=\"environment_api\",\
reason=\"deadline\"} 1\n"
        ),
        "{text}"
    );
}

/// T6：command-watch 分类与早退包装（明文 → tls_required，全流仍计 http/success）。
#[tokio::test]
async fn command_watch_classified_and_early_exit_counted() {
    let fixture = fixture().await;
    let response = send(
        fixture.data_plane,
        Request::get("/api/environment/v1/commands/watch")
            .body(Full::new(Bytes::new()))
            .unwrap(),
    )
    .await;
    let status = response.status();
    let _ = response.into_body().collect().await;
    let text = metrics_text(fixture.health).await;
    let class = status.as_u16() / 100;
    assert!(
        text.contains(
            "data_plane_ingress_full_stream_duration_seconds_count{route_class=\"command_watch\",\
transport=\"http\",outcome=\"success\"} 1\n"
        ),
        "命令通道早退响应应由处理器包装并按 command_watch 分类：\n{text}"
    );
    assert!(
        text.contains(&format!(
            "data_plane_ingress_http_responses_total{{class=\"{class}xx\"}} 1\n"
        )),
        "{text}"
    );
}

/// T7：CONNECT 隧道关闭记录（隧道后端双向干净 EOF → success）。
#[tokio::test]
async fn connect_tunnel_close_is_counted() {
    let backend = tunnel_backend().await;
    let fixture = relay_fixture(backend).await;
    let mut client = tokio::net::TcpStream::connect(fixture.data_plane)
        .await
        .unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    client
        .write_all(
            format!(
                "CONNECT instance-x:{} HTTP/1.1\r\nHost: instance-x:{}\r\n\
X-Adx-Access-Kind: port-forwarding\r\n\r\n",
                backend.port(),
                backend.port()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut buffer = [0u8; 1024];
    let mut head = String::new();
    // 读到完整响应头为止（或对端关闭）：非 200 时立即断言失败并给出实际应答，
    // 避免在 keep-alive 连接上无限等待。
    while !head.contains("\r\n\r\n") {
        let n = client.read(&mut buffer).await.unwrap();
        if n == 0 {
            break;
        }
        head.push_str(&String::from_utf8_lossy(&buffer[..n]));
    }
    assert!(
        head.starts_with("HTTP/1.1 200"),
        "隧道建立应答 200，实际：\n{head}"
    );
    // 后端关写侧 → 服务端半关 → 客户端读到 EOF（含 ready 字节）后关闭 → 双向 EOF。
    let mut tail = Vec::new();
    client.read_to_end(&mut tail).await.unwrap();
    drop(client);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let text = metrics_text(fixture.health).await;
    assert!(
        text.contains(
            "data_plane_ingress_full_stream_duration_seconds_count{route_class=\"environment_api\",\
transport=\"connect\",outcome=\"success\"} 1\n"
        ),
        "隧道正常关闭应记 transport=connect / success：\n{text}"
    );
    assert!(
        text.contains("data_plane_ingress_http_responses_total{class=\"2xx\"} 1\n"),
        "{text}"
    );
}

/// W1：经转发的业务 WS（101 升级）——升级拷贝任务在连接结束时记
/// `{ws, unknown}`（关 socket 不是完成交换的证据；success 永不导出，M1-9）。
#[tokio::test]
async fn proxied_websocket_upgrade_records_unknown_on_close() {
    let backend = ws_backend().await;
    let fixture = relay_fixture(backend).await;
    let mut client = tokio::net::TcpStream::connect(fixture.data_plane)
        .await
        .unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    client
        .write_all(
            format!(
                "GET /instance-x/{}/ws HTTP/1.1\r\nHost: ingress\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
                backend.port()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut buffer = [0u8; 1024];
    let mut head = String::new();
    while !head.contains("\r\n\r\n") {
        let n = client.read(&mut buffer).await.unwrap();
        if n == 0 {
            break;
        }
        head.push_str(&String::from_utf8_lossy(&buffer[..n]));
    }
    assert!(
        head.starts_with("HTTP/1.1 101"),
        "升级应答 101，实际：\n{head}"
    );
    // 客户端关闭 → 拷贝方向 EOF → 升级拷贝任务记 {ws, unknown}。
    drop(client);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let text = metrics_text(fixture.health).await;
    assert!(
        text.contains(
            "data_plane_ingress_full_stream_duration_seconds_count{route_class=\"environment_api\",transport=\"ws\",outcome=\"unknown\"} 1\n"
        ),
        "业务 WS 关闭应记 ws/unknown：\n{text}"
    );
    assert!(
        text.contains("data_plane_ingress_http_responses_total{class=\"1xx\"} 1\n"),
        "{text}"
    );
}
