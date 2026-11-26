use super::*;
use crate::common::protocol::GatewayPolicy;
use crate::common::route::{EnvironmentStatus, RouteInfo};
use crate::ingress::{H2PoolConfig, RouteStore};
use crate::node::Relay;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn headers(socket: &mut TcpStream) -> Vec<u8> {
    let mut result = Vec::new();
    while !result.ends_with(b"\r\n\r\n") {
        result.push(socket.read_u8().await.unwrap());
    }
    result
}

async fn fixture(port: u16, slow: bool) -> (Arc<Ingress>, Vec<tokio::task::JoinHandle<()>>) {
    let relay = Arc::new(Relay::new(GatewayPolicy::for_local_mock(vec![
        "127.0.0.0/8".parse().unwrap(),
    ])));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_address = listener.local_addr().unwrap().to_string();
    let relay_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let _ = relay.serve_h2(socket).await;
    });
    let store = Arc::new(RouteStore::new());
    store.put(RouteInfo {
        private_service_ports: vec![],
        e2b: None,
        instance_id: "environment".into(),
        tenant_id: "tenant".into(),
        environment_id: "running".into(),
        environment_ip: "127.0.0.1".into(),
        relay_address,
        environment_status: EnvironmentStatus {
            code: 3,
            ..Default::default()
        },
        tunnel_security_mode: Default::default(),
        port_forward_security_mode: Default::default(),
        port_forward_routes: vec![],
    });
    store.set_ready(true);
    let ingress = Arc::new(
        Ingress::new(
            Arc::new(IngressRouteResolver::new(store)),
            DataPlaneL4Connector::new(H2PoolConfig {
                tls_config: None,
                connections_per_node: 1,
                max_connections_per_node: 1,
                ..Default::default()
            }),
            IngressAuthenticator::disabled(),
            port,
            8765,
            "127.0.0.1:1",
            vec![],
        )
        .with_transfer_policy(if slow {
            crate::config::TransferPolicy {
                slow_after: std::time::Duration::from_millis(10),
                ..Default::default()
            }
        } else {
            Default::default()
        }),
    );
    (ingress, vec![relay_task])
}

#[tokio::test]
async fn file_trace_stays_open_until_response_body_finishes() {
    const ISOLATED: &str = "ADX_TRANSFER_TRACE_TEST_CHILD";
    if std::env::var_os(ISOLATED).is_none() {
        assert!(std::process::Command::new(std::env::current_exe().unwrap())
            .args(["ingress::server::transfer_tests::file_trace_stays_open_until_response_body_finishes", "--exact", "--nocapture"])
            .env(ISOLATED, "1").status().unwrap().success());
        return;
    }
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());
    for (method, path, mode) in [
        ("GET", "download", 0),
        ("POST", "upload", 0),
        ("GET", "download", 1),
        ("GET", "download", 2),
        ("GET", "download", 3),
        ("POST", "upload", 3),
    ] {
        exporter.reset();
        let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = backend.local_addr().unwrap().port();
        let (release, released) = tokio::sync::oneshot::channel();
        let backend_task = tokio::spawn(async move {
            let (mut socket, _) = backend.accept().await.unwrap();
            headers(&mut socket).await;
            if method == "POST" {
                let mut body = [0; 4];
                socket.read_exact(&mut body).await.unwrap();
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\na")
                .await
                .unwrap();
            let _ = released.await;
            if mode == 0 || mode == 3 {
                socket.write_all(b"bcd").await.unwrap();
            }
        });
        let (ingress, mut tasks) = fixture(port, mode == 3).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tasks.push(tokio::spawn(async move {
            let (socket, peer) = listener.accept().await.unwrap();
            ingress
                .serve_http_connection(socket, peer, IngressSecurity::Tls)
                .await;
        }));
        let mut socket = TcpStream::connect(address).await.unwrap();
        let body = if method == "POST" { "data" } else { "" };
        socket.write_all(format!("{method} /direct/environment/{path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        let response_headers = headers(&mut socket).await;
        assert!(
            response_headers.starts_with(b"HTTP/1.1 200"),
            "{}",
            String::from_utf8_lossy(&response_headers)
        );
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        let completed = exporter.get_finished_spans().unwrap();
        assert!(
            !completed
                .iter()
                .any(|span| span.name == format!("{method} /{path}")),
            "ingress file span ended at headers: {completed:?}"
        );
        if mode == 2 {
            let connection_task = tasks.pop().unwrap();
            connection_task.abort();
            let _ = connection_task.await;
            drop(socket);
            drop(release);
        } else {
            release.send(()).unwrap();
            let mut response = Vec::new();
            socket.read_to_end(&mut response).await.unwrap();
            if mode == 0 || mode == 3 {
                assert!(response.ends_with(b"abcd"));
            }
        }
        backend_task.await.unwrap();
        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let span = spans
            .iter()
            .find(|span| span.name == format!("{method} /{path}"))
            .unwrap();
        assert!(
            span.end_time.duration_since(span.start_time).unwrap()
                >= std::time::Duration::from_millis(30)
        );
        let outcome = match mode {
            0 | 3 => "complete",
            1 => "response_body_error",
            _ => "dropped",
        };
        assert!(span
            .attributes
            .iter()
            .any(|a| a.key.as_str() == "transfer.outcome" && a.value.to_string() == outcome));
        if mode == 3 {
            assert!(span
                .attributes
                .iter()
                .any(|a| a.key.as_str() == "http.request.body.size"
                    && a.value.to_string() == body.len().to_string()));
            assert!(span
                .attributes
                .iter()
                .any(|a| a.key.as_str() == "transfer.slow" && a.value.to_string() == "true"));
        } else {
            assert!(!span
                .attributes
                .iter()
                .any(|a| a.key.as_str() == "http.request.body.size"));
        }
        if mode == 1 || mode == 2 {
            assert!(matches!(
                span.status,
                opentelemetry::trace::Status::Error { .. }
            ));
        }
        for task in tasks {
            task.abort();
        }
    }
    provider.shutdown().unwrap();
}
