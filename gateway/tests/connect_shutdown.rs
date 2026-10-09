use bytes::Bytes;
use data_plane_gateway::common::protocol::ConnectTarget;
use data_plane_gateway::ingress::{DataPlaneL4Connector, H2PoolConfig};
use futures_util::future::poll_fn;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio::time::{timeout, Duration};

#[derive(Clone, Copy)]
enum Tail {
    Eof,
    Hold,
    Flood,
}

struct Peer {
    address: String,
    events: mpsc::UnboundedReceiver<Result<Option<h2::Reason>, String>>,
    connections: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn peer(tail: Tail) -> Peer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let connections = Arc::new(AtomicUsize::new(0));
    let count = connections.clone();
    let (events_tx, events) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            count.fetch_add(1, Ordering::Relaxed);
            let events_tx = events_tx.clone();
            tokio::spawn(async move {
                let mut connection = h2::server::handshake(socket).await.unwrap();
                while let Some(request) = connection.accept().await {
                    let (request, mut respond) = request.unwrap();
                    let events_tx = events_tx.clone();
                    tokio::spawn(async move {
                        let mut recv = request.into_body();
                        let mut send = respond
                            .send_response(http::Response::new(()), false)
                            .unwrap();
                        send.send_data(Bytes::from_static(b"ok"), false).unwrap();
                        let end = loop {
                            match recv.data().await {
                                Some(Ok(data)) if data.is_empty() => continue,
                                result => break result,
                            }
                        };
                        match end {
                            None => match tail {
                                Tail::Eof => {
                                    send.send_data(Bytes::new(), true).unwrap();
                                    let _ = events_tx.send(Ok(None));
                                }
                                Tail::Hold | Tail::Flood => {
                                    if matches!(tail, Tail::Flood) {
                                        send.send_data(Bytes::from(vec![b'x'; 128 * 1024]), false)
                                            .unwrap();
                                    }
                                    let reset = poll_fn(|cx| send.poll_reset(cx))
                                        .await
                                        .map(Some)
                                        .map_err(|error| error.to_string());
                                    let _ = events_tx.send(reset);
                                }
                            },
                            Some(Err(error)) => {
                                let _ = events_tx.send(Ok(error.reason()));
                            }
                            Some(Ok(_)) => {
                                let _ = events_tx.send(Err("unexpected request data".into()));
                            }
                        }
                    });
                }
            });
        }
    });
    Peer {
        address,
        events,
        connections,
        task,
    }
}

fn connector() -> DataPlaneL4Connector {
    DataPlaneL4Connector::new(H2PoolConfig {
        connections_per_node: 1,
        max_connections_per_node: 1,
        ..Default::default()
    })
}

fn target() -> ConnectTarget {
    ConnectTarget {
        instance_id: "shutdown-test".into(),
        workload_id: "backend".into(),
        target_ip: "127.0.0.1".parse().unwrap(),
        target_port: 50090,
        request_id: "shutdown-test".into(),
    }
}

async fn next_event(peer: &mut Peer) -> Option<h2::Reason> {
    timeout(Duration::from_secs(3), peer.events.recv())
        .await
        .expect("CONNECT did not release its peer")
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn normal_drop_allows_peer_to_finish_its_trailing_eof() {
    let mut peer = peer(Tail::Eof).await;
    let connector = connector();
    let (_cancel, cancelled) = watch::channel(false);
    let mut stream = connector
        .connect_stream(&peer.address, &target(), cancelled)
        .await
        .unwrap();
    let mut body = [0; 2];
    stream.read_exact(&mut body).await.unwrap();
    assert_eq!(&body, b"ok");
    // The caller knows its HTTP body is complete without reading the H2 EOF.
    drop(stream);
    assert_eq!(
        next_event(&mut peer).await,
        None,
        "normal completion sent a reset"
    );
}

#[tokio::test]
async fn normal_short_requests_reuse_one_connection_beyond_the_reset_limit() {
    let mut peer = peer(Tail::Eof).await;
    let connector = connector();
    let (_cancel, cancelled) = watch::channel(false);
    for _ in 0..1100 {
        let mut stream = connector
            .connect_stream(&peer.address, &target(), cancelled.clone())
            .await
            .unwrap();
        let mut body = [0; 2];
        stream.read_exact(&mut body).await.unwrap();
        drop(stream);
        assert_eq!(next_event(&mut peer).await, None);
    }
    assert_eq!(peer.connections.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn dropped_peer_without_eof_is_reclaimed_within_a_bounded_time() {
    let mut peer = peer(Tail::Hold).await;
    let connector = connector();
    let (_cancel, cancelled) = watch::channel(false);
    let mut stream = connector
        .connect_stream(&peer.address, &target(), cancelled)
        .await
        .unwrap();
    stream.read_exact(&mut [0; 2]).await.unwrap();
    drop(stream);
    assert_eq!(next_event(&mut peer).await, Some(h2::Reason::CANCEL));
}

#[tokio::test]
async fn dropped_peer_cannot_stream_unbounded_unread_data() {
    let mut peer = peer(Tail::Flood).await;
    let connector = connector();
    let (_cancel, cancelled) = watch::channel(false);
    let mut stream = connector
        .connect_stream(&peer.address, &target(), cancelled)
        .await
        .unwrap();
    stream.read_exact(&mut [0; 2]).await.unwrap();
    drop(stream);
    assert_eq!(next_event(&mut peer).await, Some(h2::Reason::CANCEL));
}

#[tokio::test]
async fn route_cancellation_during_close_does_not_wait_for_peer_eof() {
    let mut peer = peer(Tail::Hold).await;
    let connector = connector();
    let (cancel, cancelled) = watch::channel(false);
    let mut stream = connector
        .connect_stream(&peer.address, &target(), cancelled)
        .await
        .unwrap();
    stream.read_exact(&mut [0; 2]).await.unwrap();
    drop(stream);
    cancel.send(true).unwrap();
    assert_eq!(
        timeout(Duration::from_millis(250), next_event(&mut peer))
            .await
            .unwrap(),
        Some(h2::Reason::CANCEL)
    );
}

#[tokio::test]
async fn normal_drop_with_session_sender_released_still_finishes_gracefully() {
    let mut peer = peer(Tail::Eof).await;
    let connector = connector();
    let (cancel, cancelled) = watch::channel(false);
    let mut stream = connector
        .connect_stream(&peer.address, &target(), cancelled)
        .await
        .unwrap();
    stream.read_exact(&mut [0; 2]).await.unwrap();
    drop(stream);
    drop(cancel);
    assert_eq!(next_event(&mut peer).await, None);
}

#[tokio::test]
async fn lost_cancel_sender_still_aborts_an_active_stream() {
    let mut peer = peer(Tail::Hold).await;
    let connector = connector();
    let (cancel, cancelled) = watch::channel(false);
    let mut stream = connector
        .connect_stream(&peer.address, &target(), cancelled)
        .await
        .unwrap();
    stream.read_exact(&mut [0; 2]).await.unwrap();
    drop(cancel);
    let error = timeout(Duration::from_millis(250), stream.read(&mut [0; 1]))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::ConnectionAborted);
    drop(stream);
    assert_eq!(next_event(&mut peer).await, Some(h2::Reason::CANCEL));
}
