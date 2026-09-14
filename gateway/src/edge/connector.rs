use super::pool::{H2ConnectionPool, H2PoolConfig};
use crate::common::protocol::ConnectTarget;
use bytes::Bytes;
use futures_util::task::AtomicWaker;
use h2::{RecvStream, SendStream};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::watch;

/// Shared Edge-side L4 adapter. Every access mode opens one logical H2 CONNECT
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
        node_proxy_address: &str,
        target: &ConnectTarget,
        cancelled: watch::Receiver<bool>,
    ) -> io::Result<H2ConnectStream> {
        self.connect_stream_with_activity(node_proxy_address, target, cancelled, false)
            .await
    }

    pub async fn connect_stream_with_activity(
        &self,
        node_proxy_address: &str,
        target: &ConnectTarget,
        mut cancelled: watch::Receiver<bool>,
        passive: bool,
    ) -> io::Result<H2ConnectStream> {
        let (send, recv, pool_stream) = self
            .pool
            .connect_with_activity(node_proxy_address, target, passive)
            .await?;
        let cancellation = Arc::new(StreamCancellation::default());
        let cancellation_observer = Arc::downgrade(&cancellation);
        tokio::spawn(async move {
            loop {
                if cancelled.changed().await.is_err() || *cancelled.borrow() {
                    if let Some(cancellation) = cancellation_observer.upgrade() {
                        cancellation.cancel();
                    }
                    return;
                }
            }
        });
        Ok(H2ConnectStream {
            recv,
            send,
            read_data: Bytes::new(),
            send_closed: false,
            recv_closed: false,
            cancellation,
            _pool_stream: pool_stream,
        })
    }
}

#[derive(Default)]
struct StreamCancellation {
    cancelled: AtomicBool,
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
    recv: RecvStream,
    send: SendStream<Bytes>,
    read_data: Bytes,
    send_closed: bool,
    recv_closed: bool,
    cancellation: Arc<StreamCancellation>,
    _pool_stream: super::pool::PoolStreamGuard,
}

impl std::fmt::Debug for H2ConnectStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("H2ConnectStream")
            .field("send_closed", &self.send_closed)
            .field("recv_closed", &self.recv_closed)
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
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.cancellation.is_cancelled() {
            return Poll::Ready(Err(Self::cancelled_error()));
        }
        self.cancellation.read_waker.register(context.waker());
        if self.cancellation.is_cancelled() {
            return Poll::Ready(Err(Self::cancelled_error()));
        }

        loop {
            if !self.read_data.is_empty() {
                let amount = self.read_data.len().min(buffer.remaining());
                buffer.put_slice(&self.read_data.split_to(amount));
                self.recv
                    .flow_control()
                    .release_capacity(amount)
                    .map_err(Self::h2_error)?;
                return Poll::Ready(Ok(()));
            }
            if self.recv_closed || buffer.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }
            match self.recv.poll_data(context) {
                Poll::Ready(Some(Ok(data))) => self.read_data = data,
                Poll::Ready(Some(Err(error))) => {
                    return Poll::Ready(Err(Self::h2_error(error)));
                }
                Poll::Ready(None) => {
                    self.recv_closed = true;
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl AsyncWrite for H2ConnectStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        if self.cancellation.is_cancelled() {
            return Poll::Ready(Err(Self::cancelled_error()));
        }
        if self.send_closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "CONNECT send stream is closed",
            )));
        }
        if buffer.is_empty() {
            return Poll::Ready(Ok(0));
        }
        self.cancellation.write_waker.register(context.waker());
        if self.cancellation.is_cancelled() {
            return Poll::Ready(Err(Self::cancelled_error()));
        }

        self.send.reserve_capacity(buffer.len());
        match self.send.poll_capacity(context) {
            Poll::Ready(Some(Ok(capacity))) if capacity > 0 => {
                let amount = capacity.min(buffer.len());
                self.send
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
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        if !self.send_closed {
            self.send
                .send_data(Bytes::new(), true)
                .map_err(Self::h2_error)?;
            self.send_closed = true;
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for H2ConnectStream {
    fn drop(&mut self) {
        if !self.send_closed || !self.recv_closed {
            self.send.send_reset(h2::Reason::CANCEL);
        }
    }
}
