//! The dedicated node-local listener for execution control connections.
use super::RuntimeControlHub;
use adx_core::{runtime::RuntimeIdentity, Error};
use adx_protocol::{
    runtime::runtime_control_service_server::RuntimeControlServiceServer,
    runtime_stream::MAX_CONTROL_MESSAGE_BYTES,
};
use futures_util::StreamExt;
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    time::Sleep,
};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

struct ConnectionInner {
    cancelled: CancellationToken,
    registered: AtomicBool,
    identity: Mutex<Option<RuntimeIdentity>>,
}

/// A TCP connection belongs to one execution, including across stream replacement.
#[derive(Clone)]
pub(super) struct Connection(Arc<ConnectionInner>);
impl Connection {
    pub(super) fn claim(&self, identity: &RuntimeIdentity) -> adx_core::Result<()> {
        let mut current =
            self.0.identity.lock().map_err(|_| {
                Error::Unavailable("runtime connection ownership unavailable".into())
            })?;
        if self.0.cancelled.is_cancelled() {
            return Err(Error::Unavailable("runtime connection closed".into()));
        }
        if current.as_ref().is_some_and(|old| old != identity) {
            return Err(Error::Conflict);
        }
        *current = Some(identity.clone());
        self.0.registered.store(true, Ordering::Release);
        Ok(())
    }
    pub(super) fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub(super) fn close(&self) {
        self.0.cancelled.cancel();
    }
}

struct ExecutionIo {
    stream: TcpStream,
    connection: Connection,
    cancelled: Pin<Box<WaitForCancellationFutureOwned>>,
    registration_deadline: Option<Pin<Box<Sleep>>>,
}
impl ExecutionIo {
    fn new(stream: TcpStream, registration_timeout: Duration) -> io::Result<Self> {
        // A retired guest may already have lost its TAP/IP. Reset instead of
        // retaining its TCP tuple while FIN delivery waits on a dead endpoint.
        socket2::SockRef::from(&stream).set_linger(Some(Duration::ZERO))?;
        let token = CancellationToken::new();
        let cancelled = Box::pin(token.clone().cancelled_owned());
        Ok(Self {
            stream,
            connection: Connection(Arc::new(ConnectionInner {
                cancelled: token,
                registered: AtomicBool::new(false),
                identity: Mutex::new(None),
            })),
            cancelled,
            registration_deadline: Some(Box::pin(tokio::time::sleep(registration_timeout))),
        })
    }
    fn check(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        let closed = self.cancelled.as_mut().poll(cx).is_ready();
        let expired = if self.connection.0.registered.load(Ordering::Acquire) {
            self.registration_deadline = None;
            false
        } else {
            self.registration_deadline
                .as_mut()
                .is_some_and(|deadline| deadline.as_mut().poll(cx).is_ready())
        };
        if closed || expired {
            self.connection.close();
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "runtime control connection retired or registration expired",
            ));
        }
        Ok(())
    }
}
impl tonic::transport::server::Connected for ExecutionIo {
    type ConnectInfo = Connection;
    fn connect_info(&self) -> Self::ConnectInfo {
        self.connection.clone()
    }
}
impl AsyncRead for ExecutionIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Err(error) = self.check(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.stream).poll_read(cx, buffer)
    }
}
impl AsyncWrite for ExecutionIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Err(error) = self.check(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.stream).poll_write(cx, buffer)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Err(error) = self.check(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

impl RuntimeControlHub {
    /// Serve execution-scoped control streams, bounding unregistered transports.
    pub async fn serve(self, listener: TcpListener) -> Result<(), tonic::transport::Error> {
        self.serve_with_registration_timeout(listener, Duration::from_secs(5))
            .await
    }

    async fn serve_with_registration_timeout(
        self,
        listener: TcpListener,
        registration_timeout: Duration,
    ) -> Result<(), tonic::transport::Error> {
        let incoming =
            tokio_stream::wrappers::TcpListenerStream::new(listener).map(move |accepted| {
                accepted.and_then(|stream| ExecutionIo::new(stream, registration_timeout))
            });
        tonic::transport::Server::builder()
            .http2_keepalive_interval(Some(Duration::from_secs(30)))
            .http2_keepalive_timeout(Some(Duration::from_secs(10)))
            .add_service(
                RuntimeControlServiceServer::new(self)
                    .max_decoding_message_size(MAX_CONTROL_MESSAGE_BYTES)
                    .max_encoding_message_size(MAX_CONTROL_MESSAGE_BYTES),
            )
            .serve_with_incoming(incoming)
            .await
    }
}

#[cfg(test)]
mod tests;
