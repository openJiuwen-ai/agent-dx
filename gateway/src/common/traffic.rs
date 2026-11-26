//! Per-connection byte counters. Forwarding never takes the registry lock;
//! collection sums active counters and totals retained after connections close.
use std::collections::BTreeMap;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Clone, Default)]
pub(crate) struct ByteCounters {
    pub up: Arc<AtomicU64>,
    pub down: Arc<AtomicU64>,
}
impl ByteCounters {
    fn snapshot(&self) -> (u64, u64) {
        (
            self.up.load(Ordering::Relaxed),
            self.down.load(Ordering::Relaxed),
        )
    }
}
#[derive(Default)]
struct State {
    active: BTreeMap<usize, ByteCounters>,
    completed: (u64, u64),
}
#[derive(Default)]
pub(crate) struct TrafficMeter {
    state: Mutex<State>,
}
impl TrafficMeter {
    fn state(&self) -> MutexGuard<'_, State> {
        // Telemetry is non-authoritative; a poisoned bookkeeping lock must not
        // turn a connection teardown into an application failure.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub fn register(self: &Arc<Self>, counters: ByteCounters) -> Registration {
        // The registry retains the Arc until removal, so this identity cannot
        // be reused by another active connection. It is never exported.
        let key = Arc::as_ptr(&counters.up) as usize;
        self.state().active.insert(key, counters.clone());
        Registration {
            owner: self.clone(),
            key,
            counters,
        }
    }
    pub fn snapshot(&self) -> (u64, u64) {
        let state = self.state();
        state
            .active
            .values()
            .fold(state.completed, |(up, down), counters| {
                let (u, d) = counters.snapshot();
                (up.saturating_add(u), down.saturating_add(d))
            })
    }
}
pub(crate) struct Registration {
    owner: Arc<TrafficMeter>,
    key: usize,
    counters: ByteCounters,
}
impl Drop for Registration {
    fn drop(&mut self) {
        let mut state = self.owner.state();
        if let Some(counters) = state.active.remove(&self.key) {
            let (up, down) = counters.snapshot();
            state.completed.0 = state.completed.0.saturating_add(up);
            state.completed.1 = state.completed.1.saturating_add(down);
        }
    }
}

pub(crate) struct MeteredIo<T> {
    inner: T,
    registration: Registration,
    read: u64,
    written: u64,
    connection: Option<Arc<super::transfer::ConnectionProgress>>,
}
impl<T> MeteredIo<T> {
    pub fn new(inner: T, meter: &Arc<TrafficMeter>) -> Self {
        Self {
            inner,
            registration: meter.register(ByteCounters::default()),
            read: 0,
            written: 0,
            connection: None,
        }
    }
    pub fn with_connection(
        inner: T,
        meter: &Arc<TrafficMeter>,
        connection: Arc<super::transfer::ConnectionProgress>,
    ) -> Self {
        let mut io = Self::new(inner, meter);
        io.connection = Some(connection);
        io
    }
    fn written(&mut self, result: Poll<io::Result<usize>>) -> Poll<io::Result<usize>> {
        if let Some(connection) = &self.connection {
            if result.is_pending() {
                connection.write_wait.pending(connection.now_ms());
            } else {
                connection.write_wait.ready(connection.now_ms());
            }
        }
        if let Poll::Ready(Ok(n)) = result {
            self.written = self.written.saturating_add(n as u64);
            if let Some(connection) = &self.connection {
                connection.written.store(self.written, Ordering::Relaxed);
            }
            // One writer owns this direction. A relaxed store avoids a global
            // or per-connection read-modify-write on the forwarding path.
            self.registration
                .counters
                .down
                .store(self.written, Ordering::Relaxed);
        }
        result
    }
}
impl<T: AsyncRead + Unpin> AsyncRead for MeteredIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = result {
            self.read = self
                .read
                .saturating_add((buf.filled().len() - before) as u64);
            self.registration
                .counters
                .up
                .store(self.read, Ordering::Relaxed);
        }
        result
    }
}
impl<T: AsyncWrite + Unpin> AsyncWrite for MeteredIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write(cx, buf);
        self.written(result)
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        self.written(result)
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
#[path = "traffic_tests.rs"]
mod tests;
