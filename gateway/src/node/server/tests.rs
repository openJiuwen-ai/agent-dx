use super::*;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const INTERVAL: Duration = Duration::from_millis(60);
const PONG_TIMEOUT: Duration = Duration::from_millis(60);
const TEST_TIMEOUT: Duration = Duration::from_secs(2);

struct Link {
    client: h2::client::SendRequest<Bytes>,
    node_task: JoinHandle<Result<(), h2::Error>>,
    client_task: JoinHandle<Result<(), h2::Error>>,
    bridge: JoinHandle<()>,
    drop_to_node: Arc<AtomicBool>,
    drop_to_edge: Arc<AtomicBool>,
}

impl Drop for Link {
    fn drop(&mut self) {
        self.node_task.abort();
        self.client_task.abort();
        self.bridge.abort();
    }
}

async fn copy_or_drop<R, W>(mut reader: R, mut writer: W, drop: Arc<AtomicBool>)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut bytes = [0; 8192];
    loop {
        let Ok(n) = reader.read(&mut bytes).await else {
            break;
        };
        if n == 0 {
            break;
        }
        if !drop.load(Ordering::SeqCst) && writer.write_all(&bytes[..n]).await.is_err() {
            break;
        }
    }
    let _ = writer.shutdown().await;
}

async fn link(node: NodeProxy) -> Link {
    let (client_io, left) = tokio::io::duplex(65536);
    let (right, node_io) = tokio::io::duplex(65536);
    let drop_to_node = Arc::new(AtomicBool::new(false));
    let drop_to_edge = Arc::new(AtomicBool::new(false));
    let incoming = drop_to_node.clone();
    let outgoing = drop_to_edge.clone();
    let bridge = tokio::spawn(async move {
        let (lr, lw) = tokio::io::split(left);
        let (rr, rw) = tokio::io::split(right);
        tokio::join!(
            copy_or_drop(lr, rw, incoming),
            copy_or_drop(rr, lw, outgoing)
        );
    });
    let node_task = tokio::spawn(async move {
        node.serve_h2_with_keepalive(node_io, INTERVAL, PONG_TIMEOUT)
            .await
    });
    let (client, connection) = timeout(TEST_TIMEOUT, h2::client::handshake(client_io))
        .await
        .unwrap()
        .unwrap();
    let client_task = tokio::spawn(connection);
    Link {
        client,
        node_task,
        client_task,
        bridge,
        drop_to_node,
        drop_to_edge,
    }
}

async fn open_stream(
    link: &mut Link,
    address: SocketAddr,
) -> (h2::SendStream<Bytes>, h2::RecvStream) {
    let request = Request::builder()
        .method(http::Method::CONNECT)
        .uri(address.to_string())
        .header("x-adx-instance-id", "instance")
        .header("x-adx-workload-id", "sandbox")
        .header("x-adx-target-ip", address.ip().to_string())
        .header("x-adx-target-port", address.port().to_string())
        .header("x-adx-request-id", "request")
        .body(())
        .unwrap();
    let (response, send) = link.client.send_request(request, false).unwrap();
    let response = timeout(TEST_TIMEOUT, response).await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    (send, response.into_body())
}

fn gateway() -> NodeProxy {
    NodeProxy::new(GatewayPolicy::for_local_mock(vec!["127.0.0.0/8"
        .parse()
        .unwrap()]))
}

async fn wait_active(node: &NodeProxy, expected: usize) {
    timeout(TEST_TIMEOUT, async {
        while node.active_streams() != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("connection teardown retained a backend relay");
}

async fn blackhole(input: bool, output: bool) {
    let node = gateway();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut dead = link(node.clone()).await;
    let mut healthy = link(node.clone()).await;
    let (_dead_send, _dead_recv) = open_stream(&mut dead, address).await;
    let (mut dead_backend, _) = listener.accept().await.unwrap();
    let (mut healthy_send, mut healthy_recv) = open_stream(&mut healthy, address).await;
    let (mut healthy_backend, _) = listener.accept().await.unwrap();
    assert_eq!(node.active_streams(), 2);
    dead.drop_to_node.store(input, Ordering::SeqCst);
    dead.drop_to_edge.store(output, Ordering::SeqCst);
    let result = timeout(TEST_TIMEOUT, &mut dead.node_task)
        .await
        .expect("Node did not detect a silent H2 peer")
        .unwrap();
    assert!(result.is_err());
    wait_active(&node, 1).await;
    let mut byte = [0];
    assert_eq!(
        timeout(TEST_TIMEOUT, dead_backend.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    healthy_send
        .send_data(Bytes::from_static(b"alive"), false)
        .unwrap();
    let mut body = [0; 5];
    timeout(TEST_TIMEOUT, healthy_backend.read_exact(&mut body))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&body, b"alive");
    healthy_backend.write_all(b"pong").await.unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, healthy_recv.data())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        Bytes::from_static(b"pong")
    );
    healthy_recv.flow_control().release_capacity(4).unwrap();
    assert!(!healthy.node_task.is_finished());
    assert_eq!(node.metrics().completed_streams, 1);
}

#[tokio::test]
async fn h2_liveness_input_blackhole_cleans_only_affected_connection() {
    blackhole(true, false).await;
}

#[tokio::test]
async fn h2_liveness_output_blackhole_cleans_only_affected_connection() {
    blackhole(false, true).await;
}

#[tokio::test]
async fn h2_liveness_bidirectional_blackhole_cleans_only_affected_connection() {
    blackhole(true, true).await;
}

#[tokio::test]
async fn h2_liveness_healthy_idle_and_half_close_remain_usable() {
    let node = gateway();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut link = link(node.clone()).await;
    let (mut send, mut recv) = open_stream(&mut link, listener.local_addr().unwrap()).await;
    let (mut backend, _) = listener.accept().await.unwrap();
    send.send_data(Bytes::new(), true).unwrap();
    let mut byte = [0];
    assert_eq!(
        timeout(TEST_TIMEOUT, backend.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    tokio::time::sleep(INTERVAL * 6).await;
    assert_eq!(node.active_streams(), 1);
    assert!(!link.node_task.is_finished());
    backend.write_all(b"after-fin").await.unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, recv.data())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        Bytes::from_static(b"after-fin")
    );
    recv.flow_control().release_capacity(9).unwrap();
    backend.shutdown().await.unwrap();
    assert!(timeout(TEST_TIMEOUT, recv.data())
        .await
        .unwrap()
        .is_none_or(|v| v.unwrap().is_empty()));
    wait_active(&node, 0).await;
}

#[tokio::test]
async fn h2_liveness_parent_abort_cleans_half_closed_relay() {
    let node = gateway();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut link = link(node.clone()).await;
    let (mut send, _recv) = open_stream(&mut link, listener.local_addr().unwrap()).await;
    let (mut backend, _) = listener.accept().await.unwrap();
    send.send_data(Bytes::new(), true).unwrap();
    let mut byte = [0];
    assert_eq!(
        timeout(TEST_TIMEOUT, backend.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert_eq!(node.active_streams(), 1);
    link.node_task.abort();
    let _ = (&mut link.node_task).await;
    wait_active(&node, 0).await;
    assert_eq!(node.metrics().completed_streams, 1);
}
