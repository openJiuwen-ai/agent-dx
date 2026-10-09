//! 公共传输配置的集成测试：验证真实 accepted socket，不依赖 Node/Meta 业务。
use afs_transport::GrpcConfig;
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;

#[tokio::test]
async fn configured_incoming_applies_nodelay_to_accepted_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut incoming = GrpcConfig::default().configure_tcp_incoming(listener.into());
    let (_client, accepted) = tokio::join!(TcpStream::connect(address), incoming.next());
    let accepted = accepted.unwrap().unwrap();
    assert!(accepted.nodelay().unwrap());
}
