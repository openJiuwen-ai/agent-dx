use adx_protocol::node_proxy::{self as pb, node_proxy_service_client::NodeProxyServiceClient};
use data_plane_gateway::{
    config::{EdgeNodeSecurityMode, NodeProxyConfig},
    node::NodeProxyService,
};
use hyper_util::rt::TokioIo;
use std::time::Duration;
use tonic::transport::Endpoint;
use tower::service_fn;

#[tokio::test]
async fn shared_node_service_requires_sync_and_closes_owned_listeners() {
    let root = tempfile::Builder::new()
        .prefix("adx-node-")
        .tempdir_in("/tmp")
        .unwrap();
    let config = NodeProxyConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        advertise_address: String::new(),
        health_bind: "127.0.0.1:0".parse().unwrap(),
        allowed_target_networks: vec!["127.0.0.0/8".parse().unwrap()],
        allowed_edge_networks: vec!["127.0.0.0/8".parse().unwrap()],
        allow_any_edge: false,
        max_streams: 32,
        edge_security_mode: EdgeNodeSecurityMode::Network,
        tls_cert: String::new(),
        tls_key: String::new(),
        mtls_client_ca: String::new(),
        activity_uds_dir: Some(root.path().to_string_lossy().into_owned()),
        activity_interval: Duration::from_secs(30),
        gateway_epoch: "test".into(),
        drain_timeout: Duration::from_millis(10),
    };
    let service = NodeProxyService::bind(config).await.unwrap();
    let address = service.local_addr().unwrap();
    let (stop, done) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(service.serve(async {
        let _ = done.await;
    }));
    let socket = root.path().join("route.sock");
    let path = socket.clone();
    let channel = Endpoint::from_static("http://local")
        .connect_with_connector(service_fn(move |_| {
            let path = path.clone();
            async move {
                tokio::net::UnixStream::connect(path)
                    .await
                    .map(TokioIo::new)
            }
        }))
        .await
        .unwrap();
    let mut client = NodeProxyServiceClient::new(channel);
    let state = client
        .get_binding_state(pb::GetBindingStateRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(!state.ready);
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
    assert!(client
        .get_binding_state(pb::GetBindingStateRequest {})
        .await
        .is_err());
}
