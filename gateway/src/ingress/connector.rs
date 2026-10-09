use super::pool::{H2ConnectionPool, H2PoolConfig};
use crate::common::protocol::ConnectTarget;
use bytes::Bytes;
use futures_util::{future::poll_fn, task::AtomicWaker};
use h2::{RecvStream, SendStream};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::watch;

/// Shared Ingress-side L4 adapter. Every access mode opens one logical H2 CONNECT
/// stream from the pool and exposes it as a normal Tokio byte stream.
#[derive(Clone)]
pub struct DataPlaneL4Connector {
    pool: H2ConnectionPool,
}

impl DataPlaneL4Connector {
    pub fn new(config: H2PoolConfig) -> Self {
        Self {
            pool: H2ConnectionPool::new(config),
        }
    }

    pub fn physical_connection_count(&self) -> usize {
        self.pool.physical_connection_count()
    }

    pub async fn connect_stream(
        &self,
        relay_address: &str,
        target: &ConnectTarget,
        cancelled: watch::Receiver<bool>,
    ) -> io::Result<H2ConnectStream> {
        self.connect_stream_with_activity(relay_address, target, cancelled, false)
            .await
    }

    pub async fn connect_stream_with_activity(
        &self,
        relay_address: &str,
        target: &ConnectTarget,
        mut cancelled: watch::Receiver<bool>,
        passive: bool,
    ) -> io::Result<H2ConnectStream> {
        let (send, recv, pool_stream) = self
            .pool
            .connect_with_activity(relay_address, target, passive)
            .await?;
        let cancellation = Arc::new(StreamCancellation::default());
        let cancellation_observer = Arc::downgrade(&cancellation);
        tokio::spawn(async move {
            loop {
                let closed = cancelled.changed().await.is_err();
                let explicit = *cancelled.borrow();
                if closed || explicit {
                    if let Some(cancellation) = cancellation_observer.upgrade() {
                        if explicit || !cancellation.closing.load(Ordering::Acquire) {
                            cancellation.cancel();
                        }
                    }
                    return;
                }
            }
        });
        Ok(H2ConnectStream {
            io: Some(H2StreamIo {
                recv,
                send,
                read_data: Bytes::new(),
                send_closed: false,
                recv_closed: false,
                _pool_stream: pool_stream,
            }),
            cancellation,
        })
    }
}

#[derive(Default)]
struct StreamCancellation {
    cancelled: AtomicBool,
    closing: AtomicBool,
    read_waker: AtomicWaker,
    write_waker: AtomicWaker,
}

impl StreamCancellation {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.read_waker.wake();
        self.write_waker.wake();
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// A CONNECT stream exposed directly as Tokio I/O. This avoids the former
/// intermediate `DuplexStream`, its relay task, and one full userspace copy on
/// both directions while preserving H2 flow control and TCP half-close
/// semantics for callers such as Hyper and the raw-L4 adapters.
pub struct H2ConnectStream {
    io: Option<H2StreamIo>,
    cancellation: Arc<StreamCancellation>,
}

struct H2StreamIo {
    recv: RecvStream,
    send: SendStream<Bytes>,
    read_data: Bytes,
    send_closed: bool,
    recv_closed: bool,
    _pool_stream: super::pool::PoolStreamGuard,
}

impl std::fmt::Debug for H2ConnectStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("H2ConnectStream")
            .field("send_closed", &self.io.as_ref().map(|io| io.send_closed))
            .field("recv_closed", &self.io.as_ref().map(|io| io.recv_closed))
            .field("cancelled", &self.cancellation.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl H2ConnectStream {
    fn cancelled_error() -> io::Error {
        io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "CONNECT stream cancelled by route change",
        )
    }

    fn h2_error(error: h2::Error) -> io::Error {
        io::Error::other(error.to_string())
    }
}

impl AsyncRead for H2ConnectStream {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.cancellation.is_cancelled() {
            return Poll::Ready(Err(Self::cancelled_error()));
        }
        this.cancellation.read_waker.register(context.waker());
        if this.cancellation.is_cancelled() {
            return Poll::Ready(Err(Self::cancelled_error()));
        }
        let Some(stream) = this.io.as_mut() else {
            return Poll::Ready(Err(io::Error::other("CONNECT stream closed")));
        };

        loop {
            if !stream.read_data.is_empty() {
                let amount = stream.read_data.len().min(buffer.remaining());
                buffer.put_slice(&stream.read_data.split_to(amount));
                stream
                    .recv
                    .flow_control()
                    .release_capacity(amount)
                    .map_err(Self::h2_error)?;
                return Poll::Ready(Ok(()));
            }
            if stream.recv_closed || buffer.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }
            match stream.recv.poll_data(context) {
                Poll::Ready(Some(Ok(data))) => stream.read_data = data,
                Poll::Ready(Some(Err(error))) => {
                    return Poll::Ready(Err(Self::h2_error(error)));
                }
                Poll::Ready(None) => {
                    stream.recv_closed = true;
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl AsyncWrite for H2ConnectStream {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.get_mut();
        if this.cancellation.is_cancelled() {
            return Poll::Ready(Err(Self::cancelled_error()));
        }
        let Some(stream) = this.io.as_mut() else {
            return Poll::Ready(Err(io::Error::other("CONNECT stream closed")));
        };
        if stream.send_closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "CONNECT send stream is closed",
            )));
        }
        if buffer.is_empty() {
            return Poll::Ready(Ok(0));
        }
        this.cancellation.write_waker.register(context.waker());
        if this.cancellation.is_cancelled() {
            return Poll::Ready(Err(Self::cancelled_error()));
        }

        stream.send.reserve_capacity(buffer.len());
        match stream.send.poll_capacity(context) {
            Poll::Ready(Some(Ok(capacity))) if capacity > 0 => {
                let amount = capacity.min(buffer.len());
                stream
                    .send
                    .send_data(Bytes::copy_from_slice(&buffer[..amount]), false)
                    .map_err(Self::h2_error)?;
                Poll::Ready(Ok(amount))
            }
            Poll::Ready(Some(Ok(_))) | Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Err(error))) => Poll::Ready(Err(Self::h2_error(error))),
            Poll::Ready(None) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "CONNECT send stream was reset",
            ))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let Some(stream) = self.get_mut().io.as_mut() else {
            return Poll::Ready(Err(io::Error::other("CONNECT stream closed")));
        };
        if !stream.send_closed {
            stream
                .send
                .send_data(Bytes::new(), true)
                .map_err(Self::h2_error)?;
            stream.send_closed = true;
        }
        Poll::Ready(Ok(()))
    }
}

// Normal HTTP completion can precede the peer's H2 END_STREAM. Keep the
// transport and pool lease alive while consuming a small, bounded tail.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
const CLOSE_MAX_BYTES: usize = 64 * 1024;

impl H2StreamIo {
    async fn drain(&mut self, cancellation: &StreamCancellation) -> io::Result<()> {
        let mut drained = self.read_data.len();
        if drained > CLOSE_MAX_BYTES {
            return Err(io::Error::other("CONNECT close byte budget exhausted"));
        }
        self.recv
            .flow_control()
            .release_capacity(drained)
            .map_err(H2ConnectStream::h2_error)?;
        self.read_data = Bytes::new();
        while !self.recv_closed {
            let next = poll_fn(|context| {
                cancellation.read_waker.register(context.waker());
                if cancellation.is_cancelled() {
                    return Poll::Ready(Some(Err(H2ConnectStream::cancelled_error())));
                }
                self.recv
                    .poll_data(context)
                    .map(|data| data.map(|data| data.map_err(H2ConnectStream::h2_error)))
            })
            .await;
            match next {
                Some(Ok(data)) => {
                    if data.len() > CLOSE_MAX_BYTES - drained {
                        return Err(io::Error::other("CONNECT close byte budget exhausted"));
                    }
                    drained += data.len();
                    self.recv
                        .flow_control()
                        .release_capacity(data.len())
                        .map_err(H2ConnectStream::h2_error)?;
                }
                Some(Err(error)) => return Err(error),
                None => self.recv_closed = true,
            }
        }
        Ok(())
    }
}

impl Drop for H2ConnectStream {
    fn drop(&mut self) {
        self.cancellation.closing.store(true, Ordering::Release);
        let Some(mut stream) = self.io.take() else {
            return;
        };
        if self.cancellation.is_cancelled() {
            stream.send.send_reset(h2::Reason::CANCEL);
            return;
        }
        if !stream.send_closed {
            if stream.send.send_data(Bytes::new(), true).is_err() {
                stream.send.send_reset(h2::Reason::CANCEL);
                return;
            }
            stream.send_closed = true;
        }
        if stream.recv_closed && stream.read_data.is_empty() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            stream.send.send_reset(h2::Reason::CANCEL);
            return;
        };
        let cancellation = Arc::clone(&self.cancellation);
        runtime.spawn(async move {
            if !matches!(
                tokio::time::timeout(CLOSE_TIMEOUT, stream.drain(&cancellation)).await,
                Ok(Ok(()))
            ) {
                stream.send.send_reset(h2::Reason::CANCEL);
            }
        });
    }
}
