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
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, peer) = listener.accept().await.unwrap();
        let _ = ingress
            .serve_http_connection(socket, peer, IngressSecurity::Tls)
            .await;
    });
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket.write_all(format!("POST /direct/environment/invoke HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 2\r\nX-ADX-Request-ID: stable-request\r\n{headers}\r\n{{}}").as_bytes()).await.unwrap();
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
        let mut body = [0; 2];
        stream.read_exact(&mut body).await.unwrap();
        assert_eq!(&body, b"{}");
        assert!(!String::from_utf8(bytes)
            .unwrap()
            .to_ascii_lowercase()
            .contains("x-adx-processing-timeout-ms"));
        executions.fetch_add(1, Ordering::SeqCst);
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
async fn response_body_streaming_also_uses_the_original_processing_deadline() {
    let (ingress, tasks, count) = running_backend(1).await;
    let started = Instant::now();
    let response = request(ingress, "X-ADX-Processing-Timeout-Ms: 120\r\n").await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.ends_with('a'), "{response}");
    assert!(started.elapsed() < std::time::Duration::from_millis(220));
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
