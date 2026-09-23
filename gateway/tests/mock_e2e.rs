#![cfg(feature = "mock-e2e")]

use base64::Engine;
use data_plane_gateway::common::protocol::{ConnectTarget, GatewayPolicy};
use data_plane_gateway::common::route::{
    DataPlaneAuthMode, EnvironmentStatus, PortForwardRoute, RouteInfo,
};
use data_plane_gateway::ingress::{
    AccessKind, DataPlaneL4Connector, H2PoolConfig, Ingress, IngressAuthenticator,
    IngressRouteResolver, RouteStore,
};
use data_plane_gateway::node::Relay;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::time::{sleep, timeout, Duration};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mock_full_data_plane_protocol_matrix() {
    let (http_target, mut seen_paths, http_accepts, http_target_task) = spawn_mock_http().await;
    let (echo_target, echo_target_task) = spawn_echo().await;

    let node_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_address = node_listener.local_addr().unwrap();
    let node = Arc::new(
        Relay::new(GatewayPolicy::for_local_mock(vec!["127.0.0.0/8"
            .parse()
            .unwrap()]))
        .with_route_enforcement(),
    );
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

    let store = Arc::new(RouteStore::new());
    let route = RouteInfo {
        instance_id: "instance-a".into(),
        environment_status: EnvironmentStatus {
            code: 3,
            ..Default::default()
        },
        tenant_id: "tenant-a".into(),
        sandbox_id: "sandbox-a".into(),
        relay_address: node_address.to_string(),
        sandbox_ip: "127.0.0.1".into(),
        tunnel_security_mode: Default::default(),
        port_forward_security_mode: Default::default(),
        port_forward_routes: Vec::new(),
    };
    node.activate_route(
        route.instance_id.clone(),
        route.sandbox_id.clone(),
        route.sandbox_ip.parse().unwrap(),
    )
    .await;
    store.put(route.clone());
    let resolver = Arc::new(IngressRouteResolver::new(store.clone()));
    let connector = DataPlaneL4Connector::new(H2PoolConfig {
        connections_per_node: 1,
        max_connections_per_node: 2,
        tls_config: None,
        ..Default::default()
    });
    let node_connector = connector.clone();
    let gateway = Arc::new(
        Ingress::new(
            resolver,
            connector,
            IngressAuthenticator::new(true, false, "", Duration::from_secs(30)).unwrap(),
            http_target.port(),
            8765,
            http_target.to_string(),
            data_plane_gateway::ingress::parse_static_routes("exact:/healthz").unwrap(),
        )
        .with_client_acl(Vec::new(), true),
    );
    let ingress_http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ingress_http_address = ingress_http.local_addr().unwrap();
    let ingress_health = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ingress_health_address = ingress_health.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let route_reconciler_task =
        tokio::spawn(gateway.clone().run_route_reconciler(store.subscribe()));
    let ingress_http_task = tokio::spawn(
        gateway
            .clone()
            .serve_http(ingress_http, shutdown_rx.clone()),
    );
    let ingress_health_task =
        tokio::spawn(gateway.clone().serve_health(ingress_health, shutdown_rx));

    assert_not_ready(ingress_health_address).await;
    store.set_ready(true);
    assert_ready(ingress_health_address).await;
    assert_direct_requires_tls(ingress_http_address).await;
    assert_plaintext_rejects_credentials(ingress_http_address, http_target.port()).await;
    assert_optional_port_forwarding_without_token(ingress_http_address, http_target.port()).await;
    assert_eq!(seen_paths.recv().await.unwrap().path, "/anonymous");
    assert_sandbox_port_forward_security_override(
        ingress_http_address,
        http_target.port(),
        &store,
        &route,
    )
    .await;
    assert_port_forward_http(ingress_http_address, http_target.port()).await;
    let seen = seen_paths.recv().await.unwrap();
    assert_eq!(seen.path, "/hello?x=1");
    assert!(!seen.has_authorization);
    assert!(!seen.has_x_auth);
    assert_http_keepalive_reuse(
        ingress_http_address,
        http_target.port(),
        &mut seen_paths,
        &http_accepts,
    )
    .await;
    assert!(gateway.backend_http_pool_metrics().reused_total >= 2);
    assert_websocket_upgrade(ingress_http_address, http_target.port()).await;
    assert_eq!(seen_paths.recv().await.unwrap().path, "/socket");
    assert_http_connect(ingress_http_address, echo_target.port()).await;
    for access_kind in [
        AccessKind::Tunnel,
        AccessKind::PortForwarding,
        AccessKind::Ssh,
    ] {
        assert_connect(ingress_http_address, echo_target.port(), access_kind).await;
    }
    assert_route_change_closes_stream(ingress_http_address, echo_target.port(), &store, &route)
        .await;
    assert_environment_status_is_preserved(
        ingress_http_address,
        http_target.port(),
        &store,
        &route,
    )
    .await;
    assert_missing_route_is_404(ingress_http_address, http_target.port(), &store, &route).await;
    assert_unlistened_port_is_502(ingress_http_address).await;
    assert_node_route_retirement(&node_connector, &node, node_address, echo_target).await;
    assert_eq!(gateway.physical_connections(), 1);
    assert_node_drain(&node_connector, &node, node_address, echo_target).await;

    timeout(Duration::from_secs(2), async {
        while node.active_streams() != 0 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("node streams did not drain after ingress clients closed");
    assert_eq!(node.active_streams(), 0);
    let _ = shutdown_tx.send(true);
    ingress_http_task.await.unwrap().unwrap();
    ingress_health_task.await.unwrap().unwrap();
    route_reconciler_task.abort();
    node_task.abort();
    http_target_task.abort();
    echo_target_task.abort();
}

async fn assert_node_drain(
    connector: &DataPlaneL4Connector,
    gateway: &Relay,
    node: std::net::SocketAddr,
    target: std::net::SocketAddr,
) {
    let make_target = |workload: &str| ConnectTarget {
        instance_id: format!("{workload}-instance"),
        workload_id: workload.into(),
        target_ip: target.ip(),
        target_port: target.port(),
        request_id: format!("{workload}-request"),
    };
    gateway
        .activate_route(
            "drain-existing-instance".into(),
            "drain-existing".into(),
            target.ip(),
        )
        .await;
    gateway
        .activate_route("drain-new-instance".into(), "drain-new".into(), target.ip())
        .await;
    let (_existing_cancel, existing_cancelled) = watch::channel(false);
    let mut existing = connector
        .connect_stream(
            &node.to_string(),
            &make_target("drain-existing"),
            existing_cancelled,
        )
        .await
        .unwrap();

    gateway.start_drain();
    assert!(!gateway.ready());
    existing.write_all(b"during-drain").await.unwrap();
    let mut echoed = vec![0; "during-drain".len()];
    existing.read_exact(&mut echoed).await.unwrap();
    assert_eq!(echoed, b"during-drain");

    let (_new_cancel, new_cancelled) = watch::channel(false);
    let result = timeout(
        Duration::from_secs(2),
        connector.connect_stream(&node.to_string(), &make_target("drain-new"), new_cancelled),
    )
    .await
    .expect("new stream admission did not terminate while Node was draining");
    assert!(result.is_err());
}

async fn assert_node_route_retirement(
    connector: &DataPlaneL4Connector,
    gateway: &Relay,
    node: std::net::SocketAddr,
    target: std::net::SocketAddr,
) {
    let make_target = || ConnectTarget {
        instance_id: "route-instance".into(),
        workload_id: "route-workload".into(),
        target_ip: target.ip(),
        target_port: target.port(),
        request_id: "route-request".into(),
    };
    gateway
        .activate_route(
            "route-instance".into(),
            "route-workload".into(),
            target.ip(),
        )
        .await;
    let (_old_cancel, old_cancelled) = watch::channel(false);
    let mut old = connector
        .connect_stream(&node.to_string(), &make_target(), old_cancelled)
        .await
        .unwrap();
    old.write_all(b"old").await.unwrap();
    let mut echoed = [0; 3];
    old.read_exact(&mut echoed).await.unwrap();

    gateway
        .retire_route("route-instance".into(), "route-workload".into())
        .await;
    timeout(Duration::from_secs(2), async {
        let mut byte = [0u8; 1];
        loop {
            match old.read(&mut byte).await {
                Ok(0) | Err(_) => return,
                Ok(_) => continue,
            }
        }
    })
    .await
    .expect("route retirement did not close the old Node stream");

    let (_retired_cancel, retired_cancelled) = watch::channel(false);
    let error = connector
        .connect_stream(&node.to_string(), &make_target(), retired_cancelled)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);

    gateway
        .activate_route(
            "route-instance".into(),
            "route-workload".into(),
            target.ip(),
        )
        .await;
    let (_active_cancel, active_cancelled) = watch::channel(false);
    connector
        .connect_stream(&node.to_string(), &make_target(), active_cancelled)
        .await
        .unwrap();
}

async fn assert_not_ready(ingress: std::net::SocketAddr) {
    let response = raw_http(
        ingress,
        "GET /readyz HTTP/1.1\r\nHost: ingress\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 503"), "{response}");
}

async fn assert_ready(ingress: std::net::SocketAddr) {
    let response = raw_http(
        ingress,
        "GET /readyz HTTP/1.1\r\nHost: ingress\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
}

async fn assert_port_forward_http(ingress: std::net::SocketAddr, port: u16) {
    let response = raw_http(
        ingress,
        &format!(
            "GET /instance-a/{port}/hello?x=1 HTTP/1.1\r\nHost: ingress\r\nConnection: close\r\n\r\n"
        ),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.ends_with("pong"), "{response}");
}

async fn assert_http_keepalive_reuse(
    ingress: std::net::SocketAddr,
    port: u16,
    seen: &mut mpsc::Receiver<SeenRequest>,
    accepts: &AtomicUsize,
) {
    // Let the route reconciler consume the preceding sandbox-policy updates so
    // this assertion measures pooling rather than intentional invalidation.
    sleep(Duration::from_millis(50)).await;
    let before = accepts.load(Ordering::Relaxed);
    for path in ["reuse-a", "reuse-b"] {
        let response = raw_http(
            ingress,
            &format!(
                "GET /instance-a/{port}/{path} HTTP/1.1\r\nHost: ingress\r\nConnection: close\r\n\r\n"
            ),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert_eq!(seen.recv().await.unwrap().path, format!("/{path}"));
    }
    assert!(
        accepts.load(Ordering::Relaxed) - before <= 1,
        "two sequential requests must not open two sandbox HTTP connections"
    );
}

async fn assert_direct_requires_tls(ingress: std::net::SocketAddr) {
    let response = raw_http(
        ingress,
        "GET /direct/instance-a/hello HTTP/1.1\r\nHost: ingress\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 426"), "{response}");
}

async fn assert_plaintext_rejects_credentials(ingress: std::net::SocketAddr, port: u16) {
    let response = raw_http(
        ingress,
        &format!(
            "GET /instance-a/{port}/hello HTTP/1.1\r\nHost: ingress\r\nAuthorization: Bearer {}\r\nConnection: close\r\n\r\n",
            jwt("tenant-a")
        ),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
}

async fn assert_optional_port_forwarding_without_token(ingress: std::net::SocketAddr, port: u16) {
    let response = raw_http(
        ingress,
        &format!(
            "GET /instance-a/{port}/anonymous HTTP/1.1\r\nHost: ingress\r\nConnection: close\r\n\r\n"
        ),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
}

async fn assert_sandbox_port_forward_security_override(
    ingress: std::net::SocketAddr,
    port: u16,
    store: &RouteStore,
    route: &RouteInfo,
) {
    let mut required = route.clone();
    required.port_forward_routes = vec![PortForwardRoute {
        target_port: port,
        auth_mode: DataPlaneAuthMode::Token,
    }];
    store.put(required);

    let anonymous = raw_http(
        ingress,
        &format!(
            "GET /instance-a/{port}/sandbox-policy HTTP/1.1\r\nHost: ingress\r\nConnection: close\r\n\r\n"
        ),
    )
    .await;
    assert!(anonymous.starts_with("HTTP/1.1 426"), "{anonymous}");
    store.put(route.clone());
}

async fn assert_route_change_closes_stream(
    ingress: std::net::SocketAddr,
    port: u16,
    store: &RouteStore,
    route: &RouteInfo,
) {
    let mut stream = open_connect(ingress, port, AccessKind::Ssh).await;
    stream.write_all(b"before-route-delete").await.unwrap();
    let mut echoed = vec![0; "before-route-delete".len()];
    stream.read_exact(&mut echoed).await.unwrap();

    store.delete(&route.instance_id);
    timeout(Duration::from_secs(2), async {
        let mut byte = [0u8; 1];
        loop {
            match stream.read(&mut byte).await {
                Ok(0) | Err(_) => return,
                Ok(_) => continue,
            }
        }
    })
    .await
    .expect("deleted route stream was not closed");
    store.put(route.clone());
    assert_connect(ingress, port, AccessKind::Ssh).await;
}

async fn assert_environment_status_is_preserved(
    ingress: std::net::SocketAddr,
    port: u16,
    store: &RouteStore,
    route: &RouteInfo,
) {
    let mut failed = route.clone();
    failed.environment_status = EnvironmentStatus {
        code: 5,
        exit_code: 137,
        msg: "sandbox crashed".into(),
        kind: 9,
        err_code: 42,
    };
    store.put(failed);
    let response = raw_http(
        ingress,
        &format!(
            "GET /instance-a/{port}/hello HTTP/1.1\r\nHost: ingress\r\nConnection: close\r\n\r\n"
        ),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 409"), "{response}");
    assert!(response.contains("code=5"), "{response}");
    assert!(response.contains("sandbox crashed"), "{response}");
    assert!(response.contains("err_code=42"), "{response}");
    store.put(route.clone());
}

async fn assert_missing_route_is_404(
    ingress: std::net::SocketAddr,
    port: u16,
    store: &RouteStore,
    route: &RouteInfo,
) {
    store.delete("instance-a");
    let response = raw_http(
        ingress,
        &format!(
            "GET /instance-a/{port}/hello HTTP/1.1\r\nHost: ingress\r\nConnection: close\r\n\r\n"
        ),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 404"), "{response}");
    store.put(route.clone());
}

async fn assert_unlistened_port_is_502(ingress: std::net::SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let response = raw_http(
        ingress,
        &format!("GET /instance-a/{port}/ HTTP/1.1\r\nHost: ingress\r\nConnection: close\r\n\r\n"),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 502"), "{response}");
}

async fn assert_websocket_upgrade(ingress: std::net::SocketAddr, port: u16) {
    let mut stream = TcpStream::connect(ingress).await.unwrap();
    stream
        .write_all(
            format!(
                "GET /tunnel/instance-a/{port}/socket HTTP/1.1\r\nHost: ingress\r\nConnection: Upgrade\r\nUpgrade: mock\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let headers = read_headers(&mut stream).await;
    assert!(headers.starts_with("HTTP/1.1 101"), "{headers}");
    stream.write_all(b"websocket-bytes").await.unwrap();
    let mut echoed = vec![0; "websocket-bytes".len()];
    stream.read_exact(&mut echoed).await.unwrap();
    assert_eq!(echoed, b"websocket-bytes");
}

async fn assert_http_connect(ingress: std::net::SocketAddr, port: u16) {
    let mut stream = open_connect(ingress, port, AccessKind::PortForwarding).await;
    stream.write_all(b"connect-bytes").await.unwrap();
    let mut echoed = vec![0; "connect-bytes".len()];
    stream.read_exact(&mut echoed).await.unwrap();
    assert_eq!(echoed, b"connect-bytes");
}

async fn assert_connect(ingress: std::net::SocketAddr, port: u16, access_kind: AccessKind) {
    let mut stream = open_connect(ingress, port, access_kind).await;
    stream.write_all(b"l4-bytes").await.unwrap();
    let mut echoed = vec![0; "l4-bytes".len()];
    stream.read_exact(&mut echoed).await.unwrap();
    assert_eq!(echoed, b"l4-bytes");
}

async fn open_connect(
    ingress: std::net::SocketAddr,
    port: u16,
    access_kind: AccessKind,
) -> TcpStream {
    let mut stream = TcpStream::connect(ingress).await.unwrap();
    stream
        .write_all(
            format!(
                "CONNECT instance-a:{port} HTTP/1.1\r\nHost: instance-a:{port}\r\nX-Adx-Access-Kind: {}\r\n\r\n",
                access_kind.as_str()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let headers = read_headers(&mut stream).await;
    assert!(headers.starts_with("HTTP/1.1 200"), "{headers}");
    stream
}

fn jwt(tenant: &str) -> String {
    let payload = serde_json::json!({"sub": tenant, "exp": 0}).to_string();
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload);
    format!("e30.{payload}.signature")
}

async fn raw_http(ingress: std::net::SocketAddr, request: &str) -> String {
    let mut stream = TcpStream::connect(ingress).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    String::from_utf8(response).unwrap()
}

async fn read_headers<R>(reader: &mut R) -> String
where
    R: AsyncRead + Unpin,
{
    let mut data = Vec::new();
    let mut byte = [0u8; 1];
    while !data.ends_with(b"\r\n\r\n") {
        match reader.read(&mut byte).await {
            Ok(0) => return String::new(),
            Ok(_) => {}
            Err(_) => return String::new(),
        }
        data.push(byte[0]);
    }
    String::from_utf8(data).unwrap_or_default()
}

async fn spawn_echo() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let (mut reader, mut writer) = stream.split();
                let _ = tokio::io::copy(&mut reader, &mut writer).await;
            });
        }
    });
    (address, task)
}

async fn spawn_mock_http() -> (
    std::net::SocketAddr,
    mpsc::Receiver<SeenRequest>,
    Arc<AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (paths_tx, paths_rx) = mpsc::channel(8);
    let accepts = Arc::new(AtomicUsize::new(0));
    let task_accepts = accepts.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            task_accepts.fetch_add(1, Ordering::Relaxed);
            let paths_tx = paths_tx.clone();
            tokio::spawn(async move {
                loop {
                    let request = read_headers(&mut stream).await;
                    if request.is_empty() {
                        return;
                    }
                    let first_line = request.lines().next().unwrap_or_default();
                    let path = first_line.split_whitespace().nth(1).unwrap_or_default();
                    let lower = request.to_ascii_lowercase();
                    if paths_tx
                        .send(SeenRequest {
                            path: path.to_owned(),
                            has_authorization: lower.contains("\r\nauthorization:"),
                            has_x_auth: lower.contains("\r\nx-auth:"),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                    if lower.contains("upgrade: mock") {
                        stream
                            .write_all(
                                b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: mock\r\n\r\n",
                            )
                            .await
                            .unwrap();
                        let (mut reader, mut writer) = stream.split();
                        let _ = tokio::io::copy(&mut reader, &mut writer).await;
                        return;
                    }
                    stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: keep-alive\r\n\r\npong",
                        )
                        .await
                        .unwrap();
                }
            });
        }
    });
    (address, paths_rx, accepts, task)
}

#[derive(Debug)]
struct SeenRequest {
    path: String,
    has_authorization: bool,
    has_x_auth: bool,
}

#[tokio::test]
async fn connect_route_cancel_reclaims_half_closed_session() {
    assert_half_closed_route_cancel(false).await;
}

#[tokio::test]
async fn websocket_route_cancel_reclaims_half_closed_session() {
    assert_half_closed_route_cancel(true).await;
}

async fn assert_half_closed_route_cancel(websocket: bool) {
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_address = backend.local_addr().unwrap();
    let (received_tx, mut received_rx) = mpsc::channel(8);
    let backend_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = backend.accept().await.unwrap();
            let received = received_tx.clone();
            tokio::spawn(async move {
                if websocket {
                    read_headers(&mut stream).await;
                    stream.write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n").await.unwrap();
                }
                stream.write_all(b"ready").await.unwrap();
                stream.shutdown().await.unwrap();
                let mut byte = [0];
                while stream.read_exact(&mut byte).await.is_ok() {
                    if received.send(byte[0]).await.is_err() {
                        break;
                    }
                }
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
    let node_address = listener.local_addr().unwrap();
    let node_task = {
        let node = node.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let node = node.clone();
                tokio::spawn(async move { node.serve_h2(stream).await });
            }
        })
    };
    let store = Arc::new(RouteStore::new());
    for id in ["instance-a", "instance-b"] {
        node.activate_route(id.into(), id.into(), backend_address.ip())
            .await;
        store.put(RouteInfo {
            instance_id: id.into(),
            environment_status: EnvironmentStatus {
                code: 3,
                ..Default::default()
            },
            sandbox_id: id.into(),
            sandbox_ip: backend_address.ip().to_string(),
            relay_address: node_address.to_string(),
            tenant_id: String::new(),
            tunnel_security_mode: Default::default(),
            port_forward_security_mode: Default::default(),
            port_forward_routes: Vec::new(),
        });
    }
    store.set_ready(true);
    let ingress = Arc::new(
        Ingress::new(
            Arc::new(IngressRouteResolver::new(store.clone())),
            DataPlaneL4Connector::new(H2PoolConfig {
                connections_per_node: 1,
                max_connections_per_node: 1,
                ..Default::default()
            }),
            IngressAuthenticator::new(true, false, "", Duration::from_secs(30)).unwrap(),
            backend_address.port(),
            8765,
            backend_address.to_string(),
            Vec::new(),
        )
        .with_client_acl(Vec::new(), true),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ingress_address = listener.local_addr().unwrap();
    let (_shutdown, shutdown) = watch::channel(false);
    let reconciler = tokio::spawn(ingress.clone().run_route_reconciler(store.subscribe()));
    let ingress_task = tokio::spawn(ingress.clone().serve_http(listener, shutdown));
    let mut clients = Vec::new();
    for id in ["instance-a", "instance-b"] {
        let mut client = TcpStream::connect(ingress_address).await.unwrap();
        let port = backend_address.port();
        let request = if websocket {
            format!("GET /{id}/{port}/ws HTTP/1.1\r\nHost: ingress\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
        } else {
            format!("CONNECT {id}:{port} HTTP/1.1\r\nHost: {id}:{port}\r\nX-Adx-Access-Kind: port-forwarding\r\n\r\n")
        };
        client.write_all(request.as_bytes()).await.unwrap();
        let response = read_headers(&mut client).await;
        assert!(
            response.starts_with(if websocket {
                "HTTP/1.1 101"
            } else {
                "HTTP/1.1 200"
            }),
            "{response}"
        );
        let mut data = Vec::new();
        timeout(Duration::from_secs(3), client.read_to_end(&mut data))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(data, b"ready");
        clients.push(client);
    }
    assert_eq!(ingress.active_sessions(), 2);
    assert_eq!(ingress.physical_connections(), 1);
    // A normal upstream FIN preserves the client's writable direction.
    for (client, byte) in clients.iter_mut().zip(*b"ab") {
        client.write_all(&[byte]).await.unwrap();
        assert_eq!(
            timeout(Duration::from_secs(3), received_rx.recv())
                .await
                .unwrap(),
            Some(byte)
        );
    }
    store.delete("instance-a");
    timeout(Duration::from_secs(3), async {
        while ingress.active_sessions() != 1 || node.active_streams() != 1 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("route cancellation retained a half-closed session");
    // Keep both client sockets alive: cleanup must not depend on client close,
    // and cancelling A must not interrupt B on the shared physical H2 link.
    clients[1].write_all(b"c").await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(3), received_rx.recv())
            .await
            .unwrap(),
        Some(b'c')
    );
    assert_eq!(ingress.physical_connections(), 1);
    store.delete("instance-b");
    timeout(Duration::from_secs(3), async {
        while ingress.active_sessions() != 0 || node.active_streams() != 0 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("second route cancellation retained a half-closed session");
    drop(clients);
    ingress_task.abort();
    reconciler.abort();
    node_task.abort();
    backend_task.abort();
}
