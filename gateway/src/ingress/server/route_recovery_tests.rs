use super::*;
use crate::common::protocol::GatewayPolicy;
use crate::common::route::{EnvironmentStatus, RouteInfo};
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
        sandbox_id: "old-runtime".into(),
        relay_address: node_address.to_string(),
        sandbox_ip: "127.0.0.1".into(),
        tunnel_security_mode: Default::default(),
        port_forward_security_mode: Default::default(),
        port_forward_routes: Vec::new(),
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
        route.sandbox_id = "new-runtime".into();
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
