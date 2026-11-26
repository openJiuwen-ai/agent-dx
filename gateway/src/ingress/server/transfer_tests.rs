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
    let policy = if slow {
        crate::config::TransferPolicy {
            slow_after: std::time::Duration::from_millis(10),
            ..Default::default()
        }
    } else {
        Default::default()
    };
    (
        ingress_with_relay(port, relay_address, policy),
        vec![relay_task],
    )
}

fn ingress_with_relay(
    port: u16,
    relay_address: String,
    policy: crate::config::TransferPolicy,
) -> Arc<Ingress> {
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
    Arc::new(
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
        .with_transfer_policy(policy),
    )
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
            assert!(!span
                .attributes
                .iter()
                .any(|a| a.key.as_str().starts_with("transfer.h2_")));
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

#[tokio::test]
async fn slow_upload_reports_h2_waits_without_leaking_into_reused_download() {
    const ISOLATED: &str = "ADX_TRANSFER_H2_TRACE_TEST_CHILD";
    if std::env::var_os(ISOLATED).is_none() {
        assert!(std::process::Command::new(std::env::current_exe().unwrap())
            .args(["ingress::server::transfer_tests::slow_upload_reports_h2_waits_without_leaking_into_reused_download", "--exact", "--nocapture"])
            .env(ISOLATED, "1").status().unwrap().success());
        return;
    }
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_address = listener.local_addr().unwrap().to_string();
    let relay = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut connection = h2::server::Builder::new()
            .initial_window_size(1024)
            .handshake::<_, Bytes>(socket)
            .await
            .unwrap();
        let (request, mut response) = connection.accept().await.unwrap().unwrap();
        assert!(request
            .headers()
            .contains_key(crate::common::protocol::H_TUNNEL_ID));
        let mut recv = request.into_body();
        let mut send = response
            .send_response(http::Response::new(()), false)
            .unwrap();
        let exchange = async move {
            // Replenish small H2 windows repeatedly. Data keeps advancing, and
            // no individual wait reaches the 200ms observation window.
            let mut upload = Vec::new();
            loop {
                let data = recv.data().await.unwrap().unwrap();
                upload.extend_from_slice(&data);
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                recv.flow_control().release_capacity(data.len()).unwrap();
                if let Some(end) = upload.windows(4).position(|b| b == b"\r\n\r\n") {
                    if upload.len() >= end + 4 + 32 * 1024 {
                        break;
                    }
                }
            }
            assert!(upload.ends_with(&vec![b'x'; 32 * 1024]));
            send.send_data(
                Bytes::from_static(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndone"),
                false,
            )
            .unwrap();
            let mut download = Vec::new();
            while !download.ends_with(b"\r\n\r\n") {
                let data = recv.data().await.unwrap().unwrap();
                download.extend_from_slice(&data);
                recv.flow_control().release_capacity(data.len()).unwrap();
            }
            assert!(download.starts_with(b"GET /download "));
            send.send_data(
                Bytes::from_static(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\na"),
                false,
            )
            .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            send.send_data(Bytes::from_static(b"bcd"), false).unwrap();
            // Keep driving the transport until the frontend has consumed EOF.
            std::future::pending::<()>().await;
        };
        tokio::select! {
            _=exchange=>{},
            _=async {while connection.accept().await.is_some(){}}=>{},
        }
    });
    let ingress = ingress_with_relay(
        8766,
        relay_address,
        crate::config::TransferPolicy {
            slow_after: std::time::Duration::from_millis(200),
            ..Default::default()
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let frontend = tokio::spawn(async move {
        let (socket, peer) = listener.accept().await.unwrap();
        ingress
            .serve_http_connection(socket, peer, IngressSecurity::Tls)
            .await;
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket.write_all(b"POST /direct/environment/upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 32768\r\n\r\n").await.unwrap();
        socket.write_all(&vec![b'x'; 32 * 1024]).await.unwrap();
        assert!(headers(&mut socket).await.starts_with(b"HTTP/1.1 200"));
        let mut body = [0; 4];
        socket.read_exact(&mut body).await.unwrap();
        assert_eq!(&body, b"done");
        socket.write_all(b"GET /direct/environment/download HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").await.unwrap();
        assert!(headers(&mut socket).await.starts_with(b"HTTP/1.1 200"));
        socket.read_exact(&mut body).await.unwrap();
        assert_eq!(&body, b"abcd");
    }).await.unwrap();
    frontend.await.unwrap();
    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    let upload = spans
        .iter()
        .find(|span| span.name == "POST /upload")
        .unwrap();
    let download = spans
        .iter()
        .find(|span| span.name == "GET /download")
        .unwrap();
    let value = |span: &opentelemetry_sdk::trace::SpanData, key: &str| {
        span.attributes
            .iter()
            .find(|a| a.key.as_str() == key)
            .unwrap()
            .value
            .clone()
    };
    assert_eq!(
        value(upload, "transfer.slow_reason").to_string(),
        "low_throughput"
    );
    assert!(
        value(upload, "transfer.h2_capacity_wait_count")
            .to_string()
            .parse::<u64>()
            .unwrap()
            > 10
    );
    assert!(
        value(upload, "transfer.h2_capacity_wait_ms")
            .to_string()
            .parse::<u64>()
            .unwrap()
            > 200
    );
    assert!(
        matches!(value(upload,"transfer.h2_capacity_wait_ratio"), opentelemetry::Value::F64(ratio) if ratio > 0.5 && ratio <= 1.0)
    );
    assert!(matches!(
        value(upload, "transfer.h2_window_wait_ratio"),
        opentelemetry::Value::F64(_)
    ));
    assert_eq!(
        value(download, "transfer.h2_capacity_wait_count").to_string(),
        "0"
    );
    assert_eq!(
        value(download, "transfer.h2_capacity_wait_ms").to_string(),
        "0"
    );
    assert_eq!(
        value(upload, "transfer.h2_tunnel_id"),
        value(download, "transfer.h2_tunnel_id")
    );
    assert_eq!(
        value(upload, "transfer.h2_stream_id"),
        value(download, "transfer.h2_stream_id")
    );
    assert!(!value(upload, "transfer.h2_tunnel_id")
        .to_string()
        .is_empty());
    assert_ne!(value(upload, "transfer.h2_stream_id").to_string(), "0");
    relay.abort();
    let _ = relay.await;
    provider.shutdown().unwrap();
}
