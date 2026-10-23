use super::*;
use crate::common::protocol::{ConnectTarget, GatewayPolicy};
use crate::common::route::{
    DataPlaneAuthMode, DataPlaneSecurityMode, EnvironmentStatus, RouteInfo,
};
use crate::ingress::{DataPlaneL4Connector, H2PoolConfig, RouteStore};
use crate::node::Relay;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{timeout, Duration};

async fn binding_replacement(next_tenant: Option<&str>, change_security: bool) {
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_port = backend.local_addr().unwrap().port();
    let backend_task = tokio::spawn(async move {
        let (stream, _) = backend.accept().await.unwrap();
        let (mut reader, mut writer) = stream.into_split();
        tokio::io::copy(&mut reader, &mut writer).await.unwrap();
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
            let (stream, _) = listener.accept().await.unwrap();
            node.serve_h2(stream).await.unwrap();
        })
    };
    // Relay has already switched executions; Ingress still has the old route.
    node.activate_route(
        "environment".into(),
        "new-runtime".into(),
        "127.0.0.1".parse().unwrap(),
    )
    .await;
    let store = Arc::new(RouteStore::new());
    let mut route = RouteInfo {
        instance_id: "environment".into(),
        environment_status: EnvironmentStatus {
            code: 3,
            ..Default::default()
        },
        tenant_id: "tenant".into(),
        environment_id: "old-runtime".into(),
        relay_address: node_address.to_string(),
        environment_ip: "127.0.0.1".into(),
        tunnel_security_mode: Default::default(),
        port_forward_security_mode: Default::default(),
        port_forward_routes: Vec::new(),
        e2b: None,
    };
    store.put(route.clone());
    store.set_ready(true);
    let resolver = Arc::new(IngressRouteResolver::new(store.clone()).stream_only());
    let old = resolver
        .resolve("environment", backend_port, AccessKind::Direct, "request")
        .await
        .unwrap();
    let gateway = Arc::new(Ingress::new(
        resolver,
        DataPlaneL4Connector::new(H2PoolConfig {
            tls_config: None,
            connections_per_node: 1,
            max_connections_per_node: 1,
            ..Default::default()
        }),
        IngressAuthenticator::disabled(),
        backend_port,
        8765,
        "127.0.0.1:1",
        Vec::new(),
    ));
    let opened = tokio::spawn({
        let gateway = gateway.clone();
        async move { gateway.open_authorized_stream(old).await }
    });
    timeout(Duration::from_secs(1), async {
        while node.metrics().route_mismatch == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    if let Some(tenant) = next_tenant {
        route.environment_id = "new-runtime".into();
        route.tenant_id = tenant.into();
        if change_security {
            route.tunnel_security_mode = crate::common::route::DataPlaneSecurityMode::Tls;
        }
        store.put(route);
    }
    let result = timeout(Duration::from_secs(1), opened)
        .await
        .unwrap()
        .unwrap();
    if next_tenant == Some("tenant") && !change_security {
        let mut stream = result.unwrap();
        stream.write_all(b"ping").await.unwrap();
        let mut bytes = [0; 4];
        timeout(Duration::from_secs(1), stream.read_exact(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&bytes, b"ping");
    } else {
        assert_eq!(
            result.err().unwrap().kind(),
            if next_tenant.is_some() {
                io::ErrorKind::PermissionDenied
            } else {
                io::ErrorKind::InvalidData
            }
        );
    }
    assert_eq!(node.metrics().route_mismatch, 1);
    backend_task.abort();
    node_task.abort();
}

#[tokio::test]
async fn rejected_old_binding_waits_for_route_delta_and_opens_only_new_execution() {
    binding_replacement(Some("tenant"), false).await;
}

#[tokio::test]
async fn rejected_old_binding_does_not_reuse_authorization_for_another_tenant() {
    binding_replacement(Some("other-tenant"), false).await;
}

#[tokio::test]
async fn rejected_old_binding_does_not_reuse_changed_security_policy() {
    binding_replacement(Some("tenant"), true).await;
}

#[tokio::test]
async fn rejected_binding_without_a_delta_fails_within_the_bounded_wait() {
    binding_replacement(None, false).await;
}

struct ProbeDemand {
    paused: bool,
    wakes: std::sync::atomic::AtomicUsize,
    relay: String,
}
#[async_trait::async_trait]
impl crate::ingress::resolver::DemandResolver for ProbeDemand {
    async fn describe(
        &self,
        id: &str,
        port: u16,
        kind: AccessKind,
        request: String,
    ) -> Result<RouteHandle, ResolveError> {
        Ok(RouteHandle {
            e2b: None,
            needs_resume: self.paused,
            processing: None,
            access_kind: kind,
            relay_address: self.relay.clone(),
            tenant_id: "tenant".into(),
            tunnel_security_mode: DataPlaneSecurityMode::Tls,
            port_forward_auth_mode: DataPlaneAuthMode::None,
            target: ConnectTarget {
                instance_id: id.into(),
                workload_id: "old-runtime".into(),
                target_ip: "127.0.0.1".parse().unwrap(),
                target_port: port,
                request_id: request,
            },
        })
    }
    async fn ensure_running(&self, _: &RouteHandle) -> Result<String, ResolveError> {
        self.wakes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(ResolveError::Unavailable(
            "probe must not demand resume".into(),
        ))
    }
}

#[tokio::test]
async fn health_and_metrics_never_wake_paused_or_rejected_old_routes() {
    let relay = Arc::new(
        Relay::new(GatewayPolicy::for_local_mock(vec!["127.0.0.0/8"
            .parse()
            .unwrap()]))
        .with_route_enforcement(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_address = listener.local_addr().unwrap().to_string();
    let relay_task = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let relay = relay.clone();
            tokio::spawn(async move {
                let _ = relay.serve_h2(socket).await;
            });
        }
    });
    for paused in [true, false] {
        let store = Arc::new(RouteStore::new());
        store.set_ready(true);
        if !paused {
            store.put(RouteInfo {
                e2b: None,
                instance_id: "environment".into(),
                tenant_id: "tenant".into(),
                environment_id: "old-runtime".into(),
                environment_ip: "127.0.0.1".into(),
                relay_address: relay_address.clone(),
                environment_status: EnvironmentStatus {
                    code: 3,
                    ..Default::default()
                },
                tunnel_security_mode: DataPlaneSecurityMode::Tls,
                port_forward_security_mode: DataPlaneSecurityMode::Tls,
                port_forward_routes: vec![],
            });
        }
        let demand = Arc::new(ProbeDemand {
            paused,
            wakes: std::sync::atomic::AtomicUsize::new(0),
            relay: relay_address.clone(),
        });
        let ingress = Arc::new(Ingress::new(
            Arc::new(
                IngressRouteResolver::new(store)
                    .stream_only()
                    .with_demand_resolver(demand.clone()),
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
        for path in ["healthz", "metrics"] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let current = ingress.clone();
            let server = tokio::spawn(async move {
                let (socket, peer) = listener.accept().await.unwrap();
                let _ = current
                    .serve_http_connection(socket, peer, IngressSecurity::Plaintext)
                    .await;
            });
            let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
            client.write_all(format!("GET /environment/8080/{path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
            let mut response = Vec::new();
            timeout(Duration::from_secs(2), client.read_to_end(&mut response))
                .await
                .unwrap()
                .unwrap();
            let response = String::from_utf8(response).unwrap();
            assert!(!response.starts_with("HTTP/1.1 200"), "{response}");
            assert_eq!(demand.wakes.load(std::sync::atomic::Ordering::SeqCst), 0);
            server.abort();
        }
    }
    relay_task.abort();
}

#[tokio::test]
async fn rejected_binding_preserves_temporary_wake_failure_as_retryable() {
    let relay = Arc::new(
        Relay::new(GatewayPolicy::for_local_mock(vec!["127.0.0.0/8"
            .parse()
            .unwrap()]))
        .with_route_enforcement(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_address = listener.local_addr().unwrap().to_string();
    let relay_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let _ = relay.serve_h2(socket).await;
    });
    let store = Arc::new(RouteStore::new());
    store.set_ready(true);
    store.put(RouteInfo {
        e2b: None,
        instance_id: "environment".into(),
        tenant_id: "tenant".into(),
        environment_id: "old-runtime".into(),
        environment_ip: "127.0.0.1".into(),
        relay_address: relay_address.clone(),
        environment_status: EnvironmentStatus {
            code: 3,
            ..Default::default()
        },
        tunnel_security_mode: DataPlaneSecurityMode::Tls,
        port_forward_security_mode: DataPlaneSecurityMode::Tls,
        port_forward_routes: vec![],
    });
    let demand = Arc::new(ProbeDemand {
        paused: false,
        wakes: std::sync::atomic::AtomicUsize::new(0),
        relay: relay_address,
    });
    let resolver = Arc::new(
        IngressRouteResolver::new(store)
            .stream_only()
            .with_demand_resolver(demand.clone()),
    );
    let route = resolver
        .resolve("environment", 8088, AccessKind::Direct, "request")
        .await
        .unwrap();
    let ingress = Ingress::new(
        resolver,
        DataPlaneL4Connector::new(H2PoolConfig {
            tls_config: None,
            connections_per_node: 1,
            max_connections_per_node: 1,
            ..Default::default()
        }),
        IngressAuthenticator::disabled(),
        50090,
        8765,
        "127.0.0.1:1",
        vec![],
    );
    let error = ingress.open_authorized_stream(route).await.err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::NotConnected);
    assert_eq!(
        error_response(IngressOpenError::Connect(error)).status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(demand.wakes.load(std::sync::atomic::Ordering::SeqCst), 1);
    relay_task.abort();
}
