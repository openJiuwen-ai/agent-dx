//! Protect streaming HTTP/1 completion on the same auto connection driver as Ingress.
use bytes::Bytes;
use hyper::body::{Body, Frame};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Default)]
struct EofDuringWriteRecheck(u8);

impl Body for EofDuringWriteRecheck {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        self.0 += 1;
        // Model an EOF becoming available between the driver's two write polls.
        // No later notification is needed: the recheck observes EOF in this poll.
        // Regression described in https://github.com/hyperium/hyper/pull/4143.
        match self.0 {
            1 => Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(b"adx-body"))))),
            2 => Poll::Pending,
            _ => Poll::Ready(None),
        }
    }
}

#[tokio::test]
async fn streaming_eof_reaches_the_client_before_the_next_keepalive_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let service = service_fn(|_: Request<hyper::body::Incoming>| async {
            Ok::<_, Infallible>(Response::new(EofDuringWriteRecheck::default()))
        });
        hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
            .serve_connection_with_upgrades(TokioIo::new(socket), service)
            .await
    });
    let mut client = TcpStream::connect(address).await.unwrap();
    for sequence in 0..2 {
        client
            .write_all(b"GET /download HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut wire = Vec::new();
        let result = tokio::time::timeout(Duration::from_secs(1), async {
            let mut buffer = [0u8; 512];
            while !wire.ends_with(b"\r\n0\r\n\r\n") {
                let count = client.read(&mut buffer).await.unwrap();
                if count == 0 {
                    break;
                }
                wire.extend_from_slice(&buffer[..count]);
            }
        })
        .await;
        if result.is_err() || !wire.ends_with(b"\r\n0\r\n\r\n") {
            drop(client);
            server.abort();
            let _ = server.await;
            panic!(
                "response {sequence} did not send its chunked terminator: {:?}",
                String::from_utf8_lossy(&wire)
            );
        }
        assert!(wire.starts_with(b"HTTP/1.1 200"));
        assert!(wire.windows(b"adx-body".len()).any(|p| p == b"adx-body"));
    }
    drop(client);
    server.abort();
    let _ = server.await;
}
