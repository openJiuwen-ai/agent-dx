use super::*;
use crate::common::route::{EnvironmentStatus, RouteInfo};
use crate::ingress::e2b_route::E2bRouteConfig;
use crate::ingress::{parse_static_routes, H2PoolConfig, RouteStore};
use sha2::{Digest, Sha256};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn gateway(allow_http: bool, frontend: String, public: bool) -> Arc<Ingress> {
    let store = Arc::new(RouteStore::new());
    store.put(RouteInfo {
        instance_id: "e2b-test".into(),
        environment_status: EnvironmentStatus {
            code: 3,
            ..Default::default()
        },
        tenant_id: "tenant-a".into(),
        environment_id: "runtime-a".into(),
        environment_ip: "192.0.2.1".into(),
        relay_address: "127.0.0.1:0".into(),
        tunnel_security_mode: Default::default(),
        port_forward_security_mode: Default::default(),
        port_forward_routes: Vec::new(),
        private_service_ports: vec![49_983],
        e2b: Some(E2bRouteConfig {
            envd_token_sha256: format!("{:x}", Sha256::digest(b"envd-token")),
            traffic_token_sha256: format!("{:x}", Sha256::digest(b"traffic-token")),
            allow_public_traffic: public,
            envd_ready: true,
            ..Default::default()
        }),
    });
    store.set_ready(true);
    Arc::new(
        Ingress::new(
            Arc::new(IngressRouteResolver::new(store)),
            DataPlaneL4Connector::new(H2PoolConfig::default()),
            IngressAuthenticator::disabled(),
            50_090,
            8765,
            frontend,
            parse_static_routes(
                "prefix:/api/instances,prefix:/api/environment,prefix:/api/admin/v1/keys",
            )
            .unwrap(),
        )
        .with_port_host_domain(Some("sandbox.test".into()))
        .with_e2b_enabled(true)
        .with_e2b_allow_http(allow_http),
    )
}

async fn request(gateway: Arc<Ingress>, path: &str, headers: &str) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (socket, peer) = listener.accept().await.unwrap();
        gateway
            .serve_http_connection(socket, peer, IngressSecurity::Plaintext)
            .await;
    });
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket
        .write_all(
            format!(
                "GET {path} HTTP/1.1\r\nHost: shared.test\r\nConnection: close\r\n{headers}\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), socket.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    task.await.unwrap();
    String::from_utf8(response)
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap()
}

async fn frontend() -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = Vec::new();
        while !buffer.ends_with(b"\r\n\r\n") {
            buffer.push(socket.read_u8().await.unwrap());
        }
        let valid_key = String::from_utf8(buffer)
            .unwrap()
            .to_lowercase()
            .contains("x-api-key: test-api-key\r\n");
        let status = if valid_key {
            "200 OK"
        } else {
            "401 Unauthorized"
        };
        socket
            .write_all(
                format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
    });
    (address, task)
}

#[tokio::test]
async fn e2b_http_is_closed_without_opt_in() {
    let ingress = gateway(false, "127.0.0.1:0".into(), true);
    assert_eq!(
        request(ingress.clone(), "/sandboxes", "X-API-Key: test-api-key\r\n").await,
        426
    );
    assert_eq!(
        request(
            ingress,
            "/files?path=/tmp/test",
            "E2b-Sandbox-Id: e2b-test\r\nE2b-Sandbox-Port: 49983\r\nX-Access-Token: envd-token\r\n"
        )
        .await,
        426
    );
}

#[tokio::test]
async fn e2b_http_opt_in_forwards_management_authentication() {
    for (headers, expected) in [("", 401), ("X-API-Key: test-api-key\r\n", 200)] {
        let (address, upstream) = frontend().await;
        let status = request(gateway(true, address, true), "/sandboxes", headers).await;
        upstream.abort();
        assert_eq!(status, expected);
    }
}

#[tokio::test]
async fn e2b_http_opt_in_preserves_envd_and_private_port_authentication() {
    let ingress = gateway(true, "127.0.0.1:0".into(), false);
    let envd = "E2b-Sandbox-Id: e2b-test\r\nE2b-Sandbox-Port: 49983\r\n";
    assert_eq!(request(ingress.clone(), "/files", envd).await, 401);
    assert_eq!(
        request(
            ingress.clone(),
            "/files",
            &format!("{envd}X-Access-Token: wrong\r\n")
        )
        .await,
        401
    );
    // Correct authorization reaches the deliberately absent Relay, returning502.
    assert_eq!(
        request(
            ingress.clone(),
            "/files",
            &format!("{envd}X-Access-Token: envd-token\r\n")
        )
        .await,
        502
    );
    assert_eq!(
        request(
            ingress.clone(),
            "/init",
            &format!("{envd}X-Access-Token: envd-token\r\n")
        )
        .await,
        403
    );
    let private = "E2b-Sandbox-Id: e2b-test\r\nE2b-Sandbox-Port: 8080\r\n";
    assert_eq!(request(ingress.clone(), "/", private).await, 401);
    assert_eq!(
        request(
            ingress,
            "/",
            &format!("{private}E2b-Traffic-Access-Token: traffic-token\r\n")
        )
        .await,
        502
    );
}

#[tokio::test]
async fn e2b_http_opt_in_does_not_open_native_control_or_opaque_tunnels() {
    let ingress = gateway(true, "127.0.0.1:0".into(), true);
    for path in [
        "/api/environment/v1/resources",
        "/api/instances",
        "/api/admin/v1/keys",
    ] {
        assert_eq!(
            request(ingress.clone(), path, "X-Auth: native-key\r\n").await,
            426
        );
    }
    assert_eq!(
        request(
            ingress,
            "/",
            "E2b-Sandbox-Id: e2b-test\r\nE2b-Sandbox-Port: 50090\r\n"
        )
        .await,
        403
    );
}

#[tokio::test]
async fn e2b_http_opt_in_supports_cleartext_http2_without_opening_native_routes() {
    let (upstream_address, upstream) = frontend().await;
    let gateway = gateway(true, upstream_address, true);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (socket, peer) = listener.accept().await.unwrap();
        gateway
            .serve_http_connection(socket, peer, IngressSecurity::Plaintext)
            .await;
    });
    let (mut client, connection) =
        h2::client::handshake(TcpStream::connect(address).await.unwrap())
            .await
            .unwrap();
    let connection = tokio::spawn(connection);
    let request = Request::builder()
        .uri(format!("http://{address}/sandboxes"))
        .body(())
        .unwrap();
    let (response, _) = client.send_request(request, true).unwrap();
    let response = tokio::time::timeout(Duration::from_secs(3), response)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    connection.abort();
    task.abort();
    upstream.abort();
}

#[tokio::test]
async fn e2b_http_opt_in_rejects_noncanonical_management_paths() {
    let ingress = gateway(true, "127.0.0.1:0".into(), true);
    for path in [
        "/sandboxes/../api/admin/v1/keys",
        "/sandboxes/%2e%2e/api/admin/v1/keys",
        "/sandboxes//test",
        "/sandboxes/..%2fapi/instances",
    ] {
        assert_eq!(
            request(ingress.clone(), path, "X-API-Key: test-api-key\r\n").await,
            426
        );
    }
}
