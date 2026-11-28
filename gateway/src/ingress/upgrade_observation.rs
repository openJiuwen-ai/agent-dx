//! Optional read-only observers at an upgraded transport's accepted I/O boundaries.
//! Observation never changes bytes, backpressure, flush, cancellation or routing.
use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

type ObserveBytes = dyn Fn(&[u8]) + Send + Sync;

/// A callback must be bounded, nonblocking and must never persist message content.
#[derive(Clone)]
pub(super) struct ByteObserver(pub Arc<ObserveBytes>);

/// Created only by an authorized application entry, never from client headers.
#[derive(Clone)]
pub(super) struct UpgradeObservation {
    pub downstream_read: ByteObserver,
    pub upstream_write: ByteObserver,
    pub upstream_read: ByteObserver,
    pub downstream_write: ByteObserver,
}

pub(super) struct ObservedIo<T> {
    inner: T,
    read: ByteObserver,
    write: ByteObserver,
}

impl<T> ObservedIo<T> {
    pub fn new(inner: T, read: ByteObserver, write: ByteObserver) -> Self {
        Self { inner, read, write }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for ObservedIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) {
            (self.read.0)(&buf.filled()[before..]);
        }
        result
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for ObservedIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(count)) = result {
            (self.write.0)(&buf[..count]);
        }
        result
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        if let Poll::Ready(Ok(mut remaining)) = result {
            for buf in bufs {
                let count = remaining.min(buf.len());
                if count == 0 {
                    break;
                }
                (self.write.0)(&buf[..count]);
                remaining -= count;
            }
        }
        result
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn observer() -> (ByteObserver, Arc<Mutex<Vec<u8>>>) {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let copy = bytes.clone();
        (
            ByteObserver(Arc::new(move |input| {
                copy.lock().unwrap().extend_from_slice(input)
            })),
            bytes,
        )
    }

    #[tokio::test]
    async fn passive_observer_preserves_bytes_backpressure_and_half_close() {
        let (mut client, downstream) = tokio::io::duplex(7);
        let (upstream, mut server) = tokio::io::duplex(5);
        let (read_down, seen_down) = observer();
        let (write_up, seen_up) = observer();
        let (read_up, received_up) = observer();
        let (write_down, written_down) = observer();
        let mut downstream = ObservedIo::new(downstream, read_down, write_down);
        let mut upstream = ObservedIo::new(upstream, read_up, write_up);
        let relay = tokio::spawn(async move {
            tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await
        });
        let server = tokio::spawn(async move {
            let mut received = vec![];
            server.read_to_end(&mut received).await.unwrap();
            server.write_all(b"unchanged reply").await.unwrap();
            server.shutdown().await.unwrap();
            received
        });
        client
            .write_all(b"masked websocket bytes unchanged")
            .await
            .unwrap();
        client.shutdown().await.unwrap();
        let mut response = vec![];
        client.read_to_end(&mut response).await.unwrap();
        assert_eq!(server.await.unwrap(), b"masked websocket bytes unchanged");
        assert_eq!(response, b"unchanged reply");
        relay.await.unwrap().unwrap();
        assert_eq!(*seen_down.lock().unwrap(), *seen_up.lock().unwrap());
        assert_eq!(*received_up.lock().unwrap(), *written_down.lock().unwrap());
        assert_eq!(*received_up.lock().unwrap(), response);
    }
}
