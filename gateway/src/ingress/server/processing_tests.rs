use super::*;
use crate::common::protocol::ConnectTarget;
use crate::ingress::{resolver::DemandResolver, H2PoolConfig, RouteStore};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct SlowDemand {
    delay: std::time::Duration,
    completed: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl DemandResolver for SlowDemand {
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
            tenant_id: "tenant".into(),
            tunnel_security_mode: Default::default(),
            port_forward_auth_mode: Default::default(),
            target: ConnectTarget {
                instance_id: id.into(),
                workload_id: "old".into(),
                target_ip: "127.0.0.1".parse().unwrap(),
                target_port: port,
                request_id,
            },
        })
    }
    async fn ensure_running(&self, _: &RouteHandle) -> Result<String, ResolveError> {
        tokio::time::sleep(self.delay).await;
        self.completed.store(true, Ordering::Release);
        Ok("new".into())
    }
}

async fn request(ingress: Arc<Ingress>, headers: &str) -> String {
    send_request(ingress, &format!("POST /direct/environment/invoke HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 2\r\nX-ADX-Request-ID: stable-request\r\n{headers}\r\n{{}}")).await
}

async fn download_request(ingress: Arc<Ingress>, query: &str, headers: &str) -> String {
    send_request(ingress, &format!("GET /direct/environment/download?path=/fixture.bin{query} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nRange: bytes=5-\r\nX-ADX-Request-ID: stable-request\r\n{headers}\r\n")).await
}

async fn send_request(ingress: Arc<Ingress>, request: &str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, peer) = listener.accept().await.unwrap();
        let _ = ingress
            .serve_http_connection(socket, peer, IngressSecurity::Tls)
            .await;
    });
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut bytes = Vec::new();
    let read = tokio::time::timeout(
        std::time::Duration::from_millis(700),
        socket.read_to_end(&mut bytes),
    )
    .await;
    server.abort();
    read.expect("Ingress must return within the processing deadline")
        .unwrap();
    String::from_utf8(bytes).unwrap()
}

#[cfg(feature = "mock-e2e")]
async fn read_upload(stream: &mut TcpStream, chunked: bool) -> io::Result<Vec<u8>> {
    if !chunked {
        let mut bytes = vec![0; 4];
        stream.read_exact(&mut bytes).await?;
        return Ok(bytes);
    }
    let mut bytes = Vec::new();
    loop {
        let mut line = Vec::new();
        while !line.ends_with(b"\r\n") {
            line.push(stream.read_u8().await?);
        }
        let length = usize::from_str_radix(std::str::from_utf8(&line).unwrap().trim(), 16).unwrap();
        let start = bytes.len();
        bytes.resize(start + length, 0);
        stream.read_exact(&mut bytes[start..]).await?;
        let mut delimiter = [0; 2];
        stream.read_exact(&mut delimiter).await?;
        assert_eq!(&delimiter, b"\r\n");
        if length == 0 {
            return Ok(bytes);
        }
    }
}

fn ingress(delay: u64) -> Arc<Ingress> {
    ingress_with_progress(delay).0
}

fn ingress_with_progress(delay: u64) -> (Arc<Ingress>, Arc<AtomicBool>) {
    let completed = Arc::new(AtomicBool::new(false));
    let store = Arc::new(RouteStore::new());
    store.set_ready(true);
    let ingress = Arc::new(Ingress::new(
        Arc::new(
            IngressRouteResolver::new(store)
                .stream_only()
                .with_demand_resolver(Arc::new(SlowDemand {
                    delay: std::time::Duration::from_millis(delay),
                    completed: completed.clone(),
                })),
        ),
        DataPlaneL4Connector::new(H2PoolConfig {
            tls_config: None,
            ..Default::default()
        }),
        IngressAuthenticator::disabled(),
        50090,
        8765,
        "127.0.0.1:1",
        vec![],
    ));
    (ingress, completed)
}

#[tokio::test]
async fn pending_resume_returns_known_not_forwarded_error_before_client_timeout() {
    let response = request(ingress(1000), "X-ADX-Processing-Timeout-Ms: 120\r\n").await;
    assert!(response.starts_with("HTTP/1.1 503"), "{response}");
    assert!(
        response.to_ascii_lowercase().contains("retry-after: 1"),
        "{response}"
    );
    assert!(response.contains("ENV_RESUMING"), "{response}");
    assert!(
        response.contains("\"requestForwarded\":false"),
        "{response}"
    );
    assert!(response.contains("stable-request"), "{response}");
}

#[tokio::test]
async fn route_wait_uses_the_remaining_processing_deadline() {
    let started = Instant::now();
    let (ingress, completed) = ingress_with_progress(40);
    let response = request(ingress, "X-ADX-Processing-Timeout-Ms: 120\r\n").await;
    assert!(
        completed.load(Ordering::Acquire),
        "wake RPC must have completed before the route wait timed out"
    );
    assert!(response.contains("ENV_RESUMING"), "{response}");
    assert!(started.elapsed() < std::time::Duration::from_millis(220));
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn migration_after_a_stale_connect_uses_the_original_processing_deadline() {
    migration_after_stale_connect(false).await;
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn migration_after_stale_connect_waits_when_only_dormant_metadata_exists() {
    migration_after_stale_connect(true).await;
}

#[cfg(feature = "mock-e2e")]
async fn migration_after_stale_connect(remove_old_route: bool) {
    use crate::common::protocol::GatewayPolicy;
    use crate::common::route::{EnvironmentStatus, RouteInfo};
    use crate::node::Relay;
    use std::time::Duration;

    struct Migrating {
        store: Arc<RouteStore>,
        successor: RouteInfo,
        remove_old_route: bool,
    }

    #[async_trait::async_trait]
    impl DemandResolver for Migrating {
        async fn describe(
            &self,
            id: &str,
            port: u16,
            kind: AccessKind,
            request_id: String,
        ) -> Result<RouteHandle, ResolveError> {
            if !self.remove_old_route {
                return Err(ResolveError::NotFound);
            }
            Ok(RouteHandle {
                private_service_ports: vec![],
                e2b: None,
                needs_resume: true,
                processing: None,
                access_kind: kind,
                relay_address: self.successor.relay_address.clone(),
                tenant_id: self.successor.tenant_id.clone(),
                tunnel_security_mode: Default::default(),
                port_forward_auth_mode: Default::default(),
                target: ConnectTarget {
                    instance_id: id.into(),
                    workload_id: "old".into(),
                    target_ip: std::net::Ipv4Addr::UNSPECIFIED.into(),
                    target_port: port,
                    request_id,
                },
            })
        }

        async fn ensure_running(&self, _: &RouteHandle) -> Result<String, ResolveError> {
            // The new execution becomes routable after the old connection
            // budget elapses, but before the caller's request budget.
            tokio::time::sleep(Duration::from_millis(150)).await;
            self.store.put(self.successor.clone());
            Ok(self.successor.environment_id.clone())
        }
    }

    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = backend.local_addr().unwrap().port();
    let backend = tokio::spawn(async move {
        let (mut socket, _) = backend.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            request.push(socket.read_u8().await.unwrap());
        }
        let mut body = [0; 2];
        socket.read_exact(&mut body).await.unwrap();
        assert_eq!(&body, b"{}");
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 9\r\n\r\nmigrated!",
            )
            .await
            .unwrap();
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_address = listener.local_addr().unwrap();
    let relay = Arc::new(
        Relay::new(GatewayPolicy::for_local_mock(vec!["127.0.0.0/8"
            .parse()
            .unwrap()]))
        .with_route_enforcement(),
    );
    relay
        .activate_route(
            "environment".into(),
            "new".into(),
            "127.0.0.1".parse().unwrap(),
        )
        .await;
    let server_relay = relay.clone();
    let relay_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        server_relay.serve_h2(socket).await.unwrap();
    });
    let successor = RouteInfo {
        private_service_ports: vec![],
        instance_id: "environment".into(),
        environment_status: EnvironmentStatus {
            code: 3,
            ..Default::default()
        },
        tenant_id: "tenant".into(),
        environment_id: "new".into(),
        relay_address: relay_address.to_string(),
        environment_ip: "127.0.0.1".into(),
        tunnel_security_mode: Default::default(),
        port_forward_security_mode: Default::default(),
        port_forward_routes: vec![],
        e2b: None,
    };
    let store = Arc::new(RouteStore::new());
    store.put(RouteInfo {
        environment_id: "old".into(),
        ..successor.clone()
    });
    store.set_ready(true);
    let remove = remove_old_route.then(|| {
        let store = store.clone();
        tokio::spawn(async move {
            while relay.metrics().route_mismatch == 0 {
                tokio::task::yield_now().await;
            }
            store.delete("environment");
        })
    });
    let resolver = IngressRouteResolver::new(store.clone())
        .stream_only()
        .with_demand_resolver(Arc::new(Migrating {
            store,
            successor,
            remove_old_route,
        }));
    let gateway = Arc::new(Ingress::new(
        Arc::new(resolver),
        DataPlaneL4Connector::new(H2PoolConfig {
            connect_timeout: Duration::from_millis(100),
            connections_per_node: 1,
            max_connections_per_node: 1,
            ..Default::default()
        }),
        IngressAuthenticator::disabled(),
        port,
        8765,
        "127.0.0.1:1",
        vec![],
    ));
    let response = request(gateway, "X-ADX-Processing-Timeout-Ms: 600\r\n").await;
    if let Some(remove) = remove {
        tokio::time::timeout(Duration::from_millis(100), remove)
            .await
            .unwrap()
            .unwrap();
    }
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.ends_with("migrated!"), "{response}");
    backend.await.unwrap();
    relay_task.abort();
}

#[tokio::test]
async fn invalid_processing_deadlines_are_rejected() {
    for header in ["0", "-1", "NaN", "86400001", "999999999999999999999999"] {
        let response = request(
            ingress(1000),
            &format!("X-ADX-Processing-Timeout-Ms: {header}\r\n"),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    }
    let response = request(
        ingress(1000),
        "X-ADX-Processing-Timeout-Ms: 100\r\nX-ADX-Processing-Timeout-Ms: 200\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
}

#[cfg(feature = "mock-e2e")]
async fn running_backend(
    mode: u8,
) -> (
    Arc<Ingress>,
    Vec<tokio::task::JoinHandle<()>>,
    Arc<AtomicUsize>,
) {
    use crate::common::protocol::GatewayPolicy;
    use crate::common::route::{EnvironmentStatus, RouteInfo};
    use crate::node::Relay;
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = backend.local_addr().unwrap().port();
    let count = Arc::new(AtomicUsize::new(0));
    let executions = count.clone();
    let backend_task = tokio::spawn(async move {
        let (mut stream, _) = backend.accept().await.unwrap();
        let mut bytes = Vec::new();
        let mut byte = [0; 1];
        while !bytes.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).await.unwrap();
            bytes.extend_from_slice(&byte);
        }
        let headers = String::from_utf8(bytes.clone())
            .unwrap()
            .to_ascii_lowercase();
        assert!(!headers.contains("x-adx-processing-timeout-ms"));
        if mode == 7 || mode == 8 {
            executions.fetch_add(1, Ordering::SeqCst);
            if let Ok(body) =
                read_upload(&mut stream, headers.contains("transfer-encoding: chunked")).await
            {
                if mode == 8 {
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
                stream.write_all(&body).await.unwrap();
            }
            return;
        }
        if bytes.starts_with(b"POST ") {
            let mut body = [0; 2];
            stream.read_exact(&mut body).await.unwrap();
            assert_eq!(&body, b"{}");
        }
        executions.fetch_add(1, Ordering::SeqCst);
        if mode == 9 {
            // Model a slow application response after its request was received;
            // connection admission must no longer constrain this wait.
            tokio::time::sleep(std::time::Duration::from_millis(400)).await;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nslow")
                .await
                .unwrap();
            return;
        }
        if mode == 1 {
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\na")
                .await
                .unwrap();
        } else if mode == 2 {
            let body = "{\"error\":{\"code\":\"ENV_RESUMING\",\"requestForwarded\":false}}";
            stream.write_all(format!("HTTP/1.1 503 Unavailable\r\nX-ADX-Error-Code: ENV_RESUMING\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            return;
        }
        if (3..=5).contains(&mode) {
            let head = match mode {
                4 => "HTTP/1.1 206 Partial Content\r\nContent-Length: 4\r\nContent-Range: bytes 5-8/9\r\n\r\n",
                5 => "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 4\r\n\r\n",
                _ => "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n",
            };
            stream.write_all(head.as_bytes()).await.unwrap();
            stream.write_all(b"a").await.unwrap();
            for byte in [b"b", b"c", b"d"] {
                tokio::time::sleep(std::time::Duration::from_millis(120)).await;
                if stream.write_all(byte).await.is_err() {
                    break;
                }
            }
            return;
        }
        if mode == 6 {
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1048576\r\n\r\n")
                .await
                .unwrap();
            let _ = stream.write_all(&vec![b'x'; 1048576]).await;
            return;
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    });
    let relay = Arc::new(
        Relay::new(GatewayPolicy::for_local_mock(vec!["127.0.0.0/8"
            .parse()
            .unwrap()]))
        .with_route_enforcement(),
    );
    relay
        .activate_route(
            "environment".into(),
            "running".into(),
            "127.0.0.1".parse().unwrap(),
        )
        .await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_address = listener.local_addr().unwrap().to_string();
    let relay_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let _ = relay.serve_h2(socket).await;
    });
    let store = Arc::new(RouteStore::new());
    store.put(RouteInfo {
        private_service_ports: Vec::new(),
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
    let ingress = Arc::new(Ingress::new(
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
    ));
    (ingress, vec![backend_task, relay_task], count)
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn processing_timeout_after_forwarding_is_never_reported_as_resuming() {
    let (ingress, tasks, count) = running_backend(0).await;
    let response = request(ingress, "X-ADX-Processing-Timeout-Ms: 120\r\n").await;
    assert!(response.starts_with("HTTP/1.1 504"), "{response}");
    assert!(response.contains("\"outcome\":\"unknown\""), "{response}");
    assert!(response.contains("\"retry\":\"never\""), "{response}");
    assert!(!response.contains("ENV_RESUMING"), "{response}");
    assert_eq!(count.load(Ordering::SeqCst), 1);
    for task in tasks {
        task.abort();
    }
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn backend_cannot_assert_a_not_forwarded_resume_error() {
    let (ingress, tasks, count) = running_backend(2).await;
    let response = request(ingress, "X-ADX-Processing-Timeout-Ms: 500\r\n").await;
    assert!(response.starts_with("HTTP/1.1 503"), "{response}");
    assert!(
        !response.to_ascii_lowercase().contains("x-adx-error-code:"),
        "{response}"
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    for task in tasks {
        task.abort();
    }
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn successful_response_body_streams_past_processing_deadline() {
    let (ingress, tasks, count) = running_backend(3).await;
    let response = request(ingress, "X-ADX-Processing-Timeout-Ms: 240\r\n").await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.ends_with("abcd"), "{response}");
    assert!(!response.contains("ENV_RESUMING"));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    for task in tasks {
        task.abort();
    }
}

#[tokio::test]
async fn terminal_resume_failure_is_structured_and_cannot_be_retried_automatically() {
    let response = error_response(IngressOpenError::Resolve(ResolveError::ResumeRejected(
        "checkpoint expired".into(),
    )));
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let error: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(error["error"]["code"], "ENV_UNAVAILABLE");
    assert_eq!(error["error"]["retry"], "never");
    assert_eq!(error["error"]["requestForwarded"], false);
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn native_file_download_streams_past_processing_deadline() {
    for (mode, query, status) in [
        (3, "", "HTTP/1.1 200"),
        (3, "&type=", "HTTP/1.1 200"),
        (4, "&ty%70e=file", "HTTP/1.1 206"),
    ] {
        let (ingress, tasks, count) = running_backend(mode).await;
        let response =
            download_request(ingress, query, "X-ADX-Processing-Timeout-Ms: 240\r\n").await;
        assert!(response.starts_with(status), "{response}");
        assert!(response.ends_with("abcd"), "{response}");
        assert_eq!(count.load(Ordering::SeqCst), 1);
        for task in tasks {
            task.abort();
        }
    }
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn file_download_headers_still_use_processing_deadline() {
    let (ingress, tasks, count) = running_backend(0).await;
    let response = download_request(
        ingress,
        "&type=file",
        "X-ADX-Processing-Timeout-Ms: 120\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 504"), "{response}");
    assert!(
        response.contains("PROCESSING_DEADLINE_EXCEEDED"),
        "{response}"
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    for task in tasks {
        task.abort();
    }
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn file_download_errors_keep_processing_deadline() {
    let (ingress, tasks, _) = running_backend(5).await;
    let response = download_request(
        ingress,
        "&type=file",
        "X-ADX-Processing-Timeout-Ms: 240\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 500"), "{response}");
    let (_, body) = response.split_once("\r\n\r\n").unwrap();
    assert!(!body.is_empty() && body.len() < 4 && "abcd".starts_with(body));
    for task in tasks {
        task.abort();
    }
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn transfer_tar_download_survives_processing_deadline() {
    let (ingress, tasks, _) = running_backend(3).await;
    let response = download_request(
        ingress,
        "&ty%70e=tar",
        "X-ADX-Processing-Timeout-Ms: 240\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.ends_with("abcd"), "{response}");
    for task in tasks {
        task.abort();
    }
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn transfer_port_forwarded_download_survives_processing_deadline() {
    let (ingress, tasks, _) = running_backend(3).await;
    let port = ingress.default_direct_port;
    let response = send_request(ingress, &format!("GET /environment/{port}/download?type=file HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nX-ADX-Processing-Timeout-Ms: 240\r\n\r\n")).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.ends_with("abcd"), "{response}");
    for task in tasks {
        task.abort();
    }
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn transfer_upload_survives_processing_deadline() {
    for chunked in [false, true] {
        slow_upload(chunked).await;
    }
}

#[cfg(feature = "mock-e2e")]
async fn slow_upload(chunked: bool) {
    let (ingress, tasks, count) = running_backend(7).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, peer) = listener.accept().await.unwrap();
        ingress
            .serve_http_connection(socket, peer, IngressSecurity::Tls)
            .await;
    });
    let mut socket = TcpStream::connect(address).await.unwrap();
    let (kind, framing) = if chunked {
        ("tar", "Transfer-Encoding: chunked")
    } else {
        ("file", "Content-Length: 4")
    };
    socket.write_all(format!("POST /direct/environment/upload?type={kind}&path=/fixture HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n{framing}\r\nX-ADX-Processing-Timeout-Ms: 240\r\n\r\n").as_bytes()).await.unwrap();
    for byte in [b"a", b"b", b"c", b"d"] {
        if chunked {
            socket.write_all(b"1\r\n").await.unwrap();
        }
        if socket.write_all(byte).await.is_err() {
            break;
        }
        if chunked {
            socket.write_all(b"\r\n").await.unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    }
    if chunked {
        socket.write_all(b"0\r\n\r\n").await.unwrap();
    }
    let mut bytes = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        socket.read_to_end(&mut bytes),
    )
    .await
    .unwrap()
    .unwrap();
    let response = String::from_utf8(bytes).unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.ends_with("abcd"), "{response}");
    assert_eq!(count.load(Ordering::SeqCst), 1);
    server.abort();
    for task in tasks {
        task.abort();
    }
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn transfer_upload_stops_when_request_body_is_idle() {
    let (ingress, tasks, _) = running_backend(7).await;
    let response = send_request(ingress, "POST /direct/environment/upload?type=file&path=/fixture HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 4\r\nX-ADX-Processing-Timeout-Ms: 120\r\n\r\na").await;
    assert!(response.starts_with("HTTP/1.1 504"), "{response}");
    assert!(
        response.contains("PROCESSING_DEADLINE_EXCEEDED"),
        "{response}"
    );
    assert!(response.contains("\"outcome\":\"unknown\""), "{response}");
    assert!(response.contains("\"requestForwarded\":true"), "{response}");
    for task in tasks {
        task.abort();
    }
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn transfer_upload_keeps_deadline_while_waiting_for_response_headers() {
    let (ingress, tasks, _) = running_backend(8).await;
    let response = send_request(ingress, "POST /direct/environment/upload?type=tar&path=/fixture HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 4\r\nX-ADX-Processing-Timeout-Ms: 120\r\n\r\nabcd").await;
    assert!(response.starts_with("HTTP/1.1 504"), "{response}");
    assert!(
        response.contains("PROCESSING_DEADLINE_EXCEEDED"),
        "{response}"
    );
    for task in tasks {
        task.abort();
    }
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn native_file_download_survives_slow_downstream() {
    let (ingress, tasks, _) = running_backend(6).await;
    let (server, mut client) = tokio::io::duplex(1024);
    let connection = tokio::spawn(async move {
        ingress
            .serve_http_connection(
                server,
                "127.0.0.1:12345".parse().unwrap(),
                IngressSecurity::Tls,
            )
            .await;
    });
    client.write_all(b"GET /direct/environment/download?type=file HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nX-ADX-Processing-Timeout-Ms: 240\r\n\r\n").await.unwrap();
    let mut response = Vec::new();
    let mut buffer = [0; 1024];
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        loop {
            let count = client.read(&mut buffer).await.unwrap();
            if count == 0 {
                break;
            }
            response.extend_from_slice(&buffer[..count]);
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    connection.await.unwrap();
    let header_end = response.windows(4).position(|b| b == b"\r\n\r\n").unwrap() + 4;
    assert_eq!(&response[header_end..], vec![b'x'; 1048576]);
    for task in tasks {
        task.abort();
    }
}

// Minimal regression: during a Relay process switch the old listener is released
// before the replacement binds. A plain business request that has not yet been
// sent must wait within its original budget for the replacement to become ready,
// then execute exactly once (HTTP 200) instead of returning 502 immediately.
#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn relay_listener_absent_request_waits_and_recovers_after_rebind_executing_once() {
    relay_http_recovery(RelayRecoveryCase::Rebind).await;
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn relay_closed_bindings_request_waits_for_open_and_executes_once() {
    relay_http_recovery(RelayRecoveryCase::OpenBindings).await;
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn relay_recovery_exhausts_one_fixed_connect_budget_without_sending() {
    relay_http_recovery(RelayRecoveryCase::ConnectBudget).await;
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn relay_recovery_obeys_the_shorter_original_request_deadline() {
    relay_http_recovery(RelayRecoveryCase::RequestBudget).await;
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn relay_recovery_rejects_removed_route_before_business_send() {
    relay_http_recovery(RelayRecoveryCase::RemoveRoute).await;
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn relay_recovery_rejects_changed_tenant_before_business_send() {
    relay_http_recovery(RelayRecoveryCase::TenantChange).await;
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn relay_recovery_rejects_changed_security_before_business_send() {
    relay_http_recovery(RelayRecoveryCase::SecurityChange).await;
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn relay_recovery_stops_on_ingress_drain_before_business_send() {
    relay_http_recovery(RelayRecoveryCase::IngressDrain).await;
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn relay_backend_connect_502_is_permanent_and_never_retried() {
    relay_http_recovery(RelayRecoveryCase::BackendRefused).await;
}

#[cfg(feature = "mock-e2e")]
#[tokio::test]
async fn relay_connect_budget_does_not_truncate_sent_requests_waiting_for_response_headers() {
    let (mut ingress, tasks, count) = running_backend(9).await;
    Arc::get_mut(&mut ingress).unwrap().connector = DataPlaneL4Connector::new(H2PoolConfig {
        tls_config: None,
        connections_per_node: 1,
        max_connections_per_node: 1,
        connect_timeout: std::time::Duration::from_millis(150),
        ..Default::default()
    });
    let port = ingress.default_direct_port;
    let response = send_request(ingress, &format!(
        "GET /environment/{port}/cgi-bin/state HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nX-ADX-Processing-Timeout-Ms: 600\r\n\r\n"
    )).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.ends_with("slow"), "{response}");
    assert_eq!(count.load(Ordering::SeqCst), 1);
    for task in tasks {
        task.abort();
    }
}

#[cfg(feature = "mock-e2e")]
#[derive(Clone, Copy, Debug)]
enum RelayRecoveryCase {
    Rebind,
    OpenBindings,
    ConnectBudget,
    RequestBudget,
    RemoveRoute,
    TenantChange,
    SecurityChange,
    IngressDrain,
    BackendRefused,
}

#[cfg(feature = "mock-e2e")]
async fn relay_http_recovery(case: RelayRecoveryCase) {
    use crate::common::protocol::GatewayPolicy;
    use crate::common::route::{EnvironmentStatus, RouteInfo};
    use crate::node::{route_control::proto as pb, route_control::BindingService, Relay};
    use tokio::sync::{oneshot, watch, Notify};
    use tokio::task::{JoinHandle, JoinSet};

    #[derive(Clone)]
    struct RetryLog(Arc<Mutex<Vec<u8>>>, Arc<Notify>);
    impl io::Write for RetryLog {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            self.1.notify_one();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let log = RetryLog(Arc::new(Mutex::new(Vec::new())), Arc::new(Notify::new()));
    let writer = log.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .without_time()
        .with_writer(move || writer.clone())
        .finish();
    let _subscriber = tracing::subscriber::set_default(subscriber);

    struct AlreadyRunning {
        wakes: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl DemandResolver for AlreadyRunning {
        async fn describe(
            &self,
            _: &str,
            _: u16,
            _: AccessKind,
            _: String,
        ) -> Result<RouteHandle, ResolveError> {
            Err(ResolveError::NotFound)
        }
        async fn ensure_running(&self, route: &RouteHandle) -> Result<String, ResolveError> {
            assert_eq!(route.target.instance_id, "environment");
            assert_eq!(route.target.workload_id, "running");
            self.wakes.fetch_add(1, Ordering::SeqCst);
            Ok(route.target.workload_id.clone())
        }
    }

    fn relay() -> Arc<Relay> {
        Arc::new(
            Relay::new(GatewayPolicy::for_local_mock(vec!["127.0.0.0/8"
                .parse()
                .unwrap()]))
            .with_route_enforcement(),
        )
    }

    async fn install(relay: Arc<Relay>, candidate: bool) -> (BindingService, pb::BindingState) {
        let bindings = BindingService::new(relay);
        let mut initial = bindings.inspect().await;
        initial.controller_session_id = "diagnostic-controller".into();
        let state = bindings.begin(initial).await.unwrap();
        let snapshot = pb::BindingSnapshot {
            proxy_session_id: state.proxy_session_id.clone(),
            sync_epoch: state.sync_epoch,
            bindings: vec![pb::UpdateBindingRequest {
                proxy_session_id: state.proxy_session_id,
                sync_epoch: state.sync_epoch,
                environment_id: "environment".into(),
                ownership_generation: 1,
                binding_revision: 1,
                binding: Some(pb::update_binding_request::Binding::Active(
                    pb::RuntimeTarget {
                        runtime_id: "running".into(),
                        ip: "127.0.0.1".into(),
                    },
                )),
            }],
        };
        let state = if candidate {
            bindings.install_candidate(snapshot).await.unwrap()
        } else {
            bindings.replace(snapshot).await.unwrap()
        };
        (bindings, state)
    }

    fn accept_relay(
        listener: TcpListener,
        relay: Arc<Relay>,
    ) -> (JoinHandle<()>, oneshot::Sender<()>, oneshot::Receiver<()>) {
        let (close, mut closing) = oneshot::channel();
        let (released, release_ack) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut closing => break,
                    accepted = listener.accept() => {
                        let (socket, _) = accepted.unwrap();
                        let relay = relay.clone();
                        connections.spawn(async move {
                            relay.serve_h2(socket).await.unwrap();
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
            drop(listener);
            let _ = released.send(());
            while let Some(result) = connections.join_next().await {
                result.unwrap();
            }
        });
        (task, close, release_ack)
    }

    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = backend.local_addr().unwrap().port();
    let business_requests = Arc::new(AtomicUsize::new(0));
    let business_bytes = Arc::new(AtomicUsize::new(0));
    let request_ids = Arc::new(Mutex::new(Vec::new()));
    let backend_requests = business_requests.clone();
    let backend_bytes = business_bytes.clone();
    let backend_ids = request_ids.clone();
    let (close_backend, mut closing_backend) = oneshot::channel();
    let (backend_released, backend_release_ack) = oneshot::channel();
    let backend_task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                _ = &mut closing_backend => break,
                accepted = backend.accept() => {
                    let (mut socket, _) = accepted.unwrap();
                    let counter = backend_requests.clone();
                    let bytes = backend_bytes.clone();
                    let ids = backend_ids.clone();
                    connections.spawn(async move {
                        loop {
                            let mut head = Vec::new();
                            while !head.ends_with(b"\r\n\r\n") {
                                let mut byte = [0u8; 1];
                                if socket.read_exact(&mut byte).await.is_err() {
                                    return;
                                }
                                bytes.fetch_add(1, Ordering::SeqCst);
                                head.push(byte[0]);
                            }
                            let head = String::from_utf8(head).unwrap();
                            assert!(head.starts_with("GET /cgi-bin/state HTTP/1.1\r\n"));
                            let id = head.lines().find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("x-request-id").then(|| value.trim().to_owned())
                            }).unwrap();
                            ids.lock().unwrap().push(id);
                            counter.fetch_add(1, Ordering::SeqCst);
                            let body = "state:ok";
                            let response = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                                body.len()
                            );
                            if socket.write_all(response.as_bytes()).await.is_err() {
                                return;
                            }
                        }
                    });
                }
                _ = connections.join_next(), if !connections.is_empty() => {}
            }
        }
        drop(backend);
        let _ = backend_released.send(());
        while let Some(result) = connections.join_next().await {
            result.unwrap();
        }
    });

    let old = relay();
    let (_old_bindings, old_state) = install(old.clone(), false).await;
    assert!(old_state.ready && old.ready());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_address = listener.local_addr().unwrap();
    let (old_task, close_old, old_released) = accept_relay(listener, old.clone());

    let store = Arc::new(RouteStore::new());
    let route_info = RouteInfo {
        private_service_ports: Vec::new(),
        e2b: None,
        instance_id: "environment".into(),
        tenant_id: "tenant".into(),
        environment_id: "running".into(),
        environment_ip: "127.0.0.1".into(),
        relay_address: relay_address.to_string(),
        environment_status: EnvironmentStatus {
            code: 3,
            ..Default::default()
        },
        tunnel_security_mode: Default::default(),
        port_forward_security_mode: Default::default(),
        port_forward_routes: vec![],
    };
    store.put(route_info.clone());
    store.set_ready(true);
    let wake_calls = Arc::new(AtomicUsize::new(0));
    let ingress = Arc::new(Ingress::new(
        Arc::new(
            IngressRouteResolver::new(store.clone())
                .stream_only()
                .with_demand_resolver(Arc::new(AlreadyRunning {
                    wakes: wake_calls.clone(),
                })),
        ),
        DataPlaneL4Connector::new(H2PoolConfig {
            tls_config: None,
            connections_per_node: 1,
            max_connections_per_node: 1,
            connect_timeout: std::time::Duration::from_millis(250),
            ..Default::default()
        }),
        IngressAuthenticator::disabled(),
        port,
        8765,
        "127.0.0.1:1",
        vec![],
    ));

    let warmup = send_request(
        ingress.clone(),
        &format!(
            "GET /environment/{port}/cgi-bin/state HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nX-ADX-Request-ID: gap-warmup\r\n\r\n"
        ),
    )
    .await;
    assert!(warmup.starts_with("HTTP/1.1 200"), "{warmup}");
    assert_eq!(business_requests.load(Ordering::SeqCst), 1);
    let baseline_bytes = business_bytes.load(Ordering::SeqCst);

    let (_keep_cancel_sender, cancelled) = watch::channel(false);
    let witness = ingress
        .connector
        .connect_stream(
            &relay_address.to_string(),
            &ConnectTarget {
                instance_id: "environment".into(),
                workload_id: "running".into(),
                target_ip: "127.0.0.1".parse().unwrap(),
                target_port: port,
                request_id: "gap-goaway-witness".into(),
            },
            cancelled,
        )
        .await
        .unwrap();
    let guard = witness.reuse_guard().unwrap();
    assert!(guard.is_reusable());
    old.start_drain();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while guard.is_reusable() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the Ingress physical H2 connection must receive the old Relay GOAWAY");
    close_old.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), old_released)
        .await
        .unwrap()
        .unwrap();
    drop(witness);

    if matches!(case, RelayRecoveryCase::BackendRefused) {
        close_backend.send(()).unwrap();
        backend_release_ack.await.unwrap();
    }
    let candidate = relay();
    let (candidate_bindings, closed) = install(candidate.clone(), true).await;
    assert!(!closed.ready && !candidate.ready());
    let mut candidates = Vec::new();
    if matches!(
        case,
        RelayRecoveryCase::OpenBindings | RelayRecoveryCase::BackendRefused
    ) {
        candidates.push(accept_relay(
            TcpListener::bind(relay_address).await.unwrap(),
            candidate.clone(),
        ));
        if matches!(case, RelayRecoveryCase::BackendRefused) {
            candidate_bindings
                .open_candidate(closed.clone())
                .await
                .unwrap();
        }
    }

    // Recover only after observing an actual failed admission from this request,
    // rather than assuming a spawned request ran during a fixed delay.
    let started = tokio::time::Instant::now();
    let request_ingress = ingress.clone();
    let request_port = port;
    let gap_request = tokio::spawn(async move {
        let deadline = if matches!(case, RelayRecoveryCase::RequestBudget) {
            "X-ADX-Processing-Timeout-Ms: 100\r\n"
        } else {
            ""
        };
        send_request(
            request_ingress,
            &format!(
                "GET /environment/{request_port}/cgi-bin/state HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nX-ADX-Request-ID: gap-A\r\n{deadline}\r\n"
            ),
        )
        .await
    });
    if !matches!(case, RelayRecoveryCase::BackendRefused) {
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let changed = log.1.notified();
                let observed = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
                if observed.contains("relay_http_connect_retry")
                    && observed.contains("request_id=gap-A")
                {
                    println!("case={case:?} observed genuine Relay retry:\n{observed}");
                    break;
                }
                changed.await;
            }
        })
        .await
        .expect("must observe the actual failed Relay admission before allowing recovery");
        assert!(!gap_request.is_finished(), "request must still be waiting");
        assert_eq!(business_requests.load(Ordering::SeqCst), 1);
        assert_eq!(business_bytes.load(Ordering::SeqCst), baseline_bytes);
    }

    match case {
        RelayRecoveryCase::RemoveRoute => store.delete("environment"),
        RelayRecoveryCase::TenantChange => {
            let mut changed = route_info.clone();
            changed.tenant_id = "another-tenant".into();
            store.put(changed);
        }
        RelayRecoveryCase::SecurityChange => {
            let mut changed = route_info.clone();
            changed
                .port_forward_routes
                .push(crate::common::route::PortForwardRoute {
                    target_port: port,
                    auth_mode: crate::common::route::DataPlaneAuthMode::Token,
                });
            store.put(changed);
        }
        RelayRecoveryCase::IngressDrain => ingress.start_drain(),
        _ => {}
    }
    if !matches!(
        case,
        RelayRecoveryCase::ConnectBudget
            | RelayRecoveryCase::RequestBudget
            | RelayRecoveryCase::BackendRefused
    ) {
        if !matches!(case, RelayRecoveryCase::OpenBindings) {
            candidates.push(accept_relay(
                TcpListener::bind(relay_address).await.unwrap(),
                candidate.clone(),
            ));
        } else {
            assert!(wake_calls.load(Ordering::SeqCst) >= 1);
        }
        let opened = candidate_bindings.open_candidate(closed).await.unwrap();
        assert!(opened.ready && candidate.ready());
    }

    let recovered = tokio::time::timeout(std::time::Duration::from_secs(5), gap_request)
        .await
        .expect("request must recover within the processing budget")
        .unwrap();
    println!("case={case:?} request_id=gap-A response={recovered:?}");
    match case {
        RelayRecoveryCase::Rebind | RelayRecoveryCase::OpenBindings => {
            assert!(
                recovered.starts_with("HTTP/1.1 200"),
                "recovered: {recovered}"
            );
            assert!(recovered.ends_with("state:ok"), "recovered: {recovered}");
            assert_eq!(
                business_requests.load(Ordering::SeqCst),
                2,
                "business must execute exactly once"
            );
            assert_eq!(
                request_ids.lock().unwrap().as_slice(),
                ["gap-warmup", "gap-A"]
            );
        }
        _ => {
            assert!(!recovered.starts_with("HTTP/1.1 200"), "{recovered}");
            assert_eq!(business_requests.load(Ordering::SeqCst), 1);
            assert_eq!(business_bytes.load(Ordering::SeqCst), baseline_bytes);
            assert_eq!(request_ids.lock().unwrap().as_slice(), ["gap-warmup"]);
            if matches!(case, RelayRecoveryCase::ConnectBudget) {
                assert!(started.elapsed() >= std::time::Duration::from_millis(250));
                assert!(started.elapsed() < std::time::Duration::from_millis(700));
            }
            if matches!(case, RelayRecoveryCase::RequestBudget) {
                assert!(recovered.starts_with("HTTP/1.1 504"), "{recovered}");
                assert!(
                    recovered.contains("\"requestForwarded\":false"),
                    "{recovered}"
                );
                assert!(started.elapsed() < std::time::Duration::from_millis(250));
            }
            if matches!(
                case,
                RelayRecoveryCase::TenantChange | RelayRecoveryCase::SecurityChange
            ) {
                assert!(recovered.starts_with("HTTP/1.1 403"), "{recovered}");
            }
            if matches!(case, RelayRecoveryCase::BackendRefused) {
                assert!(recovered.starts_with("HTTP/1.1 502"), "{recovered}");
                assert!(!String::from_utf8(log.0.lock().unwrap().clone())
                    .unwrap()
                    .contains("relay_http_connect_retry"));
                assert_eq!(candidate.metrics().connect_errors, 1);
                assert!(started.elapsed() < std::time::Duration::from_millis(250));
            }
        }
    }

    backend_task.abort();
    old_task.abort();
    for (task, _, _) in candidates {
        task.abort();
    }
}
