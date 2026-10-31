use super::*;
use adx_core::runtime::{RuntimeIdentity, RuntimePhase, RuntimeStatus};
use adx_protocol::{runtime as pb, runtime_stream::execution_token};
use hyper_util::rt::TokioIo;
use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    sync::{mpsc, Notify},
};
use tokio_stream::wrappers::ReceiverStream;

struct ObservedIo {
    stream: TcpStream,
    closed: Arc<Notify>,
}
impl AsyncRead for ObservedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buffer.filled().len();
        let result = Pin::new(&mut self.stream).poll_read(cx, buffer);
        if matches!(result, Poll::Ready(Err(_)))
            || matches!(result, Poll::Ready(Ok(()))) && buffer.filled().len() == before
        {
            self.closed.notify_one();
        }
        result
    }
}
impl AsyncWrite for ObservedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buffer)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

fn status() -> RuntimeStatus {
    RuntimeStatus {
        identity: RuntimeIdentity {
            environment_id: "transport-proof".into(),
            runtime_id: "transport-proof-1".into(),
            ownership_generation: 1,
        },
        revision: 1,
        phase: RuntimePhase::Running,
        checkpoint: None,
        requested_checkpoint: None,
        requested_checkpoint_deadline_unix_millis: None,
        required_services: Default::default(),
        failure: None,
        active_requests: 0,
        active_commands: 0,
        activity_revision: 1,
    }
}

async fn connected_client(
    address: std::net::SocketAddr,
) -> (
    pb::runtime_control_service_client::RuntimeControlServiceClient<tonic::transport::Channel>,
    Arc<Notify>,
) {
    let closed = Arc::new(Notify::new());
    let observer = closed.clone();
    let channel = tonic::transport::Endpoint::new(format!("http://{address}"))
        .unwrap()
        .connect_with_connector(tower::service_fn(move |_| {
            let closed = observer.clone();
            async move {
                Ok::<_, io::Error>(TokioIo::new(ObservedIo {
                    stream: TcpStream::connect(address).await?,
                    closed,
                }))
            }
        }))
        .await
        .unwrap();
    (
        pb::runtime_control_service_client::RuntimeControlServiceClient::new(channel),
        closed,
    )
}

async fn hello(
    client: &mut pb::runtime_control_service_client::RuntimeControlServiceClient<
        tonic::transport::Channel,
    >,
    secret: &[u8],
    state: &RuntimeStatus,
) -> (
    mpsc::Sender<pb::RuntimeEvent>,
    tonic::Streaming<pb::RuntimeCommand>,
) {
    let (events, receiver) = mpsc::channel(4);
    events
        .send(pb::RuntimeEvent {
            event: Some(pb::runtime_event::Event::Hello(pb::RuntimeHello {
                token: execution_token(secret, &state.identity).unwrap(),
                status_json: serde_json::to_vec(state).unwrap(),
            })),
        })
        .await
        .unwrap();
    let commands = client
        .open_control(ReceiverStream::new(receiver))
        .await
        .unwrap()
        .into_inner();
    (events, commands)
}

#[tokio::test]
async fn registered_execution_outlives_the_registration_deadline() {
    let secret = vec![7; 32];
    let hub = RuntimeControlHub::new(secret.clone(), Duration::from_secs(1)).unwrap();
    let state = status();
    hub.expect(&state.identity).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(
        hub.clone()
            .serve_with_registration_timeout(listener, Duration::from_millis(100)),
    );
    let (mut client, closed) = connected_client(address).await;
    let peer = hello(&mut client, &secret, &state).await;
    hub.wait_ready(&state.identity).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(10), closed.notified())
            .await
            .is_err()
    );
    assert_eq!(hub.observed(&state.identity).unwrap().unwrap(), state);
    hub.retire(&state.identity);
    tokio::time::timeout(Duration::from_millis(300), closed.notified())
        .await
        .unwrap();
    drop((peer, client));
    server.abort();
}

#[tokio::test]
async fn stream_replacement_on_one_connection_keeps_the_replacement_alive() {
    let secret = vec![7; 32];
    let hub = RuntimeControlHub::new(secret.clone(), Duration::from_secs(1)).unwrap();
    let mut state = status();
    hub.expect(&state.identity).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(hub.clone().serve(listener));
    let (mut client, closed) = connected_client(address).await;
    let first = hello(&mut client, &secret, &state).await;
    state.revision += 1;
    let second = hello(&mut client, &secret, &state).await;
    hub.wait_ready(&state.identity).await.unwrap();
    drop(first);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), closed.notified())
            .await
            .is_err()
    );
    assert_eq!(hub.observed(&state.identity).unwrap().unwrap().revision, 2);
    hub.retire(&state.identity);
    tokio::time::timeout(Duration::from_millis(300), closed.notified())
        .await
        .unwrap();
    drop((second, client));
    server.abort();
}

#[tokio::test]
async fn replacement_connection_releases_old_tcp_without_closing_new_tcp() {
    let secret = vec![7; 32];
    let hub = RuntimeControlHub::new(secret.clone(), Duration::from_secs(1)).unwrap();
    let mut state = status();
    hub.expect(&state.identity).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(hub.clone().serve(listener));
    let (mut first_client, first_closed) = connected_client(address).await;
    let first = hello(&mut first_client, &secret, &state).await;
    let (mut second_client, second_closed) = connected_client(address).await;
    state.revision += 1;
    let second = hello(&mut second_client, &secret, &state).await;
    tokio::time::timeout(Duration::from_millis(300), first_closed.notified())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), second_closed.notified())
            .await
            .is_err()
    );
    assert_eq!(hub.observed(&state.identity).unwrap().unwrap().revision, 2);
    hub.retire(&state.identity);
    tokio::time::timeout(Duration::from_millis(300), second_closed.notified())
        .await
        .unwrap();
    drop((first, second, first_client, second_client));
    server.abort();
}

#[tokio::test]
async fn another_execution_cannot_share_an_owned_tcp_connection() {
    let secret = vec![7; 32];
    let hub = RuntimeControlHub::new(secret.clone(), Duration::from_secs(1)).unwrap();
    let state = status();
    let mut other = state.clone();
    other.identity.runtime_id = "transport-proof-2".into();
    other.identity.ownership_generation = 2;
    hub.expect(&state.identity).unwrap();
    hub.expect(&other.identity).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(hub.clone().serve(listener));
    let (mut client, closed) = connected_client(address).await;
    let first = hello(&mut client, &secret, &state).await;
    let (send, receive) = mpsc::channel(4);
    send.send(pb::RuntimeEvent {
        event: Some(pb::runtime_event::Event::Hello(pb::RuntimeHello {
            token: execution_token(&secret, &other.identity).unwrap(),
            status_json: serde_json::to_vec(&other).unwrap(),
        })),
    })
    .await
    .unwrap();
    assert_eq!(
        client
            .open_control(ReceiverStream::new(receive))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), closed.notified())
            .await
            .is_err()
    );
    assert!(hub.observed(&state.identity).unwrap().is_some());
    hub.retire(&state.identity);
    hub.retire(&other.identity);
    tokio::time::timeout(Duration::from_millis(300), closed.notified())
        .await
        .unwrap();
    drop((first, client, send));
    server.abort();
}

#[tokio::test]
async fn retired_execution_closes_its_tcp_connection_with_client_channel_alive() {
    let secret = vec![7; 32];
    let hub = RuntimeControlHub::new(secret.clone(), Duration::from_secs(1)).unwrap();
    let state = status();
    hub.expect(&state.identity).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(hub.clone().serve(listener));
    let closed = Arc::new(Notify::new());
    let observer = closed.clone();
    let channel = tonic::transport::Endpoint::new(format!("http://{address}"))
        .unwrap()
        .connect_with_connector(tower::service_fn(move |_| {
            let closed = observer.clone();
            async move {
                Ok::<_, io::Error>(TokioIo::new(ObservedIo {
                    stream: TcpStream::connect(address).await?,
                    closed,
                }))
            }
        }))
        .await
        .unwrap();
    let mut client = pb::runtime_control_service_client::RuntimeControlServiceClient::new(channel);
    let (events, receiver) = mpsc::channel(4);
    events
        .send(pb::RuntimeEvent {
            event: Some(pb::runtime_event::Event::Hello(pb::RuntimeHello {
                token: execution_token(&secret, &state.identity).unwrap(),
                status_json: serde_json::to_vec(&state).unwrap(),
            })),
        })
        .await
        .unwrap();
    let mut commands = client
        .open_control(ReceiverStream::new(receiver))
        .await
        .unwrap()
        .into_inner();
    hub.wait_ready(&state.identity).await.unwrap();
    hub.retire(&state.identity);
    let _ = tokio::time::timeout(Duration::from_secs(1), commands.message())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(300), closed.notified())
            .await
            .is_ok(),
        "Retiring the gRPC stream must also release its underlying TCP tuple"
    );
    drop((events, commands, client));
    server.abort();
}

#[tokio::test]
async fn tcp_peer_without_a_runtime_hello_is_closed_without_http2_keepalive() {
    let hub = RuntimeControlHub::new(vec![7; 32], Duration::from_secs(1)).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server =
        tokio::spawn(hub.serve_with_registration_timeout(listener, Duration::from_millis(50)));
    let mut peer = TcpStream::connect(address).await.unwrap();
    let mut received = [0u8; 1024];
    let completion = tokio::time::timeout(Duration::from_millis(300), async {
        loop {
            match tokio::io::AsyncReadExt::read(&mut peer, &mut received).await {
                Ok(0) | Err(_) => break,
                Ok(_) => (),
            }
        }
    })
    .await;
    assert!(
        completion.is_ok(),
        "An unregistered TCP peer must not survive indefinitely before HTTP/2/Hello completes"
    );
    server.abort();
}
