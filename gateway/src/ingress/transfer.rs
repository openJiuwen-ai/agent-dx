//! File HTTP spans remain owned by their response body, including error and drop paths.
use super::server::ProxyBody;
use crate::common::transfer::{TransferProgress, TransferRegistration};
use adx_observability::trace::Trace;
use bytes::Bytes;
use hyper::body::{Body, Frame, SizeHint};
use std::pin::Pin;
use std::sync::{atomic::Ordering, Arc};
use std::task::{Context, Poll};

pub(super) struct TransferGuard {
    trace: Option<Trace>,
    registration: Option<TransferRegistration>,
}
impl TransferGuard {
    pub fn new(trace: Trace, registration: Option<TransferRegistration>) -> Self {
        Self {
            trace: Some(trace),
            registration,
        }
    }
    pub fn trace(&self) -> &Trace {
        self.trace
            .as_ref()
            .expect("an active transfer owns its trace")
    }
    pub fn progress(&self) -> Arc<TransferProgress> {
        self.registration
            .as_ref()
            .expect("file response guards own a transfer registration")
            .progress
            .clone()
    }
    pub fn finish(&mut self, outcome: &'static str) {
        let Some(trace) = self.trace.take() else {
            return;
        };
        let Some(mut registration) = self.registration.take() else {
            trace.end();
            return;
        };
        registration.finish(outcome);
        let state = &registration.progress;
        trace.attribute("transfer.outcome", outcome.into());
        trace.integer_attribute(
            "transfer.headers_ms",
            integer(state.headers_ms.load(Ordering::Relaxed)),
        );
        if state.slow.load(Ordering::Relaxed) {
            trace.boolean_attribute("transfer.slow", true);
            trace.integer_attribute(
                "http.request.body.size",
                integer(state.request_bytes.load(Ordering::Relaxed)),
            );
            trace.integer_attribute(
                "http.response.body.size",
                integer(state.response_bytes.load(Ordering::Relaxed)),
            );
            let now = state.now_ms();
            trace.integer_attribute(
                "transfer.request_wait_ms",
                integer(state.request_wait.snapshot(now).0),
            );
            trace.integer_attribute(
                "transfer.response_wait_ms",
                integer(state.response_wait.snapshot(now).0),
            );
        }
        if outcome != "complete" {
            trace.record_error("transfer", outcome);
        }
        trace.end();
    }
}
impl Drop for TransferGuard {
    fn drop(&mut self) {
        self.finish("dropped");
    }
}
fn integer(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

pub(super) struct RequestBody<B> {
    inner: B,
    state: Arc<TransferProgress>,
}
impl<B> RequestBody<B> {
    pub fn new(inner: B, state: Arc<TransferProgress>) -> Self {
        Self { inner, state }
    }
}
impl<B: Body<Data = Bytes> + Unpin> Body for RequestBody<B> {
    type Data = Bytes;
    type Error = B::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, B::Error>>> {
        let result = Pin::new(&mut self.inner).poll_frame(cx);
        let now = self.state.now_ms();
        if result.is_pending() {
            self.state.request_wait.pending(now);
        } else {
            self.state.request_wait.ready(now);
            if let Poll::Ready(Some(Ok(ref frame))) = result {
                if let Some(bytes) = frame.data_ref() {
                    self.state.progress(true, bytes.len());
                }
            }
        }
        result
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

pub(super) struct ResponseBody {
    inner: ProxyBody,
    guard: TransferGuard,
}
impl ResponseBody {
    pub fn new(inner: ProxyBody, mut guard: TransferGuard) -> Self {
        if inner.is_end_stream() {
            guard.finish("complete");
        }
        Self { inner, guard }
    }
}
impl Body for ResponseBody {
    type Data = Bytes;
    type Error = <ProxyBody as Body>::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let result = Pin::new(&mut self.inner).poll_frame(cx);
        let state = self.guard.progress();
        let now = state.now_ms();
        match &result {
            Poll::Pending => state.response_wait.pending(now),
            Poll::Ready(Some(Ok(frame))) => {
                state.response_wait.ready(now);
                if let Some(bytes) = frame.data_ref() {
                    state.progress(false, bytes.len());
                }
                if self.inner.is_end_stream() {
                    self.guard.finish("complete");
                }
            }
            Poll::Ready(None) => {
                state.response_wait.ready(now);
                self.guard.finish("complete");
            }
            Poll::Ready(Some(Err(_))) => {
                state.response_wait.ready(now);
                self.guard.finish("response_body_error");
            }
        }
        result
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
