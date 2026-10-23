//! Command terminal notifications through an authenticated Execd WebSocket and real Relay.
use super::*;
use crate::{common::protocol::GatewayPolicy, node::Relay};
use std::time::Duration;

struct RuntimeAuth;
impl tokio_tungstenite::tungstenite::handshake::server::Callback for RuntimeAuth {
    fn on_request(
        self,
        request: &tokio_tungstenite::tungstenite::handshake::server::Request,
        response: tokio_tungstenite::tungstenite::handshake::server::Response,
    ) -> Result<
        tokio_tungstenite::tungstenite::handshake::server::Response,
        tokio_tungstenite::tungstenite::handshake::server::ErrorResponse,
    > {
        if request
            .headers()
            .get("x-auth")
            .and_then(|v| v.to_str().ok())
            != Some("runtime-only-secret")
        {
            return Err(http::Response::builder()
                .status(401)
                .body(Some("authentication required".into()))
                .unwrap());
        }
        assert!(request.headers().get("authorization").is_none());
        assert_eq!(request.uri().path(), "/commands/watch");
        Ok(response)
    }
}

#[tokio::test]
async fn command_watch_authenticates_runtime_and_delivers_terminal_state() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let runtime_task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut ws = tokio_tungstenite::accept_hdr_async(stream, RuntimeAuth)
                    .await
                    .unwrap();
                while let Some(Ok(message)) = ws.next().await {
                    if let tokio_tungstenite::tungstenite::Message::Text(text) = message {
                        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                        if value["op"] == "subscribe" {
                            ws.send(tokio_tungstenite::tungstenite::Message::Text(serde_json::json!({"op":"state","commandId":"command-a","status":"SUCCEEDED","stateVersion":1}).to_string())).await.unwrap();
                        }
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
    let address = listener.local_addr().unwrap();
    let relay_task = {
        let node = node.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let node = node.clone();
                tokio::spawn(async move {
                    let _ = node.serve_h2(stream).await;
                });
            }
        })
    };
    let backend = Arc::new(RuntimeBackend {
        queries: std::sync::atomic::AtomicUsize::new(0),
        inner: Backend::default(),
        port,
    });
    let (gateway, api, routes) = fixture_with_backend(backend.clone()).await;
    let gateway = Arc::new(with_files(&gateway, backend));
    let id = create_environment(api.as_ref()).await;
    let route: crate::common::route::RouteInfo = serde_json::from_value(serde_json::json!({"instanceID":id,"instanceStatus":{"code":3},"tenantID":"tenant","environmentID":"runtime","environmentIP":"127.0.0.1","nodeProxyAddress":address.to_string()})).unwrap();
    node.activate_route(
        route.instance_id.clone(),
        route.environment_id.clone(),
        route.environment_ip.parse().unwrap(),
    )
    .await;
    routes.put(route);
    let reconcile = tokio::spawn(gateway.clone().run_route_reconciler(routes.subscribe()));
    let route = gateway
        .resolve_route(&id, port, AccessKind::Direct, "watch-test".into())
        .await
        .unwrap();
    gateway.authorize(&route, "tenant").unwrap();
    let mut subscription = gateway
        .command_watch
        .subscribe(&gateway, route, "command-a".into(), "tenant")
        .await
        .unwrap();
    let state = tokio::time::timeout(Duration::from_secs(5), subscription.events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.command_id, "command-a");
    assert_eq!(state.status, "SUCCEEDED");
    reconcile.abort();
    relay_task.abort();
    runtime_task.abort();
}
