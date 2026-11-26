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
    pub fn progress(&self) -> &TransferProgress {
        self.registration
            .as_ref()
            .expect("file response guards own a transfer registration")
            .progress
            .as_ref()
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
        if state.slow.load(Ordering::Relaxed) && trace.is_recording() {
            let h2 = state.h2_snapshot();
            let window = state.window();
            trace.boolean_attribute("transfer.slow", true);
            trace.attribute("transfer.slow_reason", state.reason().to_owned());
            let (tunnel_id, stream_id) = state.h2_identity();
            trace.attribute("transfer.h2_tunnel_id", tunnel_id);
            trace.integer_attribute("transfer.h2_stream_id", i64::from(stream_id));
            trace.integer_attribute("transfer.h2_capacity_wait_ms", integer(h2.wait_ms));
            trace.integer_attribute("transfer.h2_capacity_wait_count", integer(h2.wait_count));
            trace.integer_attribute("transfer.h2_capacity_pending_ms", integer(h2.pending_ms));
            trace.integer_attribute("transfer.h2_enqueued_bytes", integer(h2.enqueued_bytes));
            trace.double_attribute(
                "transfer.h2_capacity_wait_ratio",
                crate::common::transfer::ratio(h2.wait_ms, state.now_ms()),
            );
            trace.integer_attribute("transfer.window_ms", integer(window.duration_ms));
            trace.integer_attribute("transfer.window_end_ms", integer(window.end_ms));
            trace.integer_attribute(
                "transfer.window_request_bytes",
                integer(window.request_bytes),
            );
            trace.integer_attribute(
                "transfer.window_response_bytes",
                integer(window.response_bytes),
            );
            trace.integer_attribute(
                "transfer.h2_window_enqueued_bytes",
                integer(window.h2_enqueued_bytes),
            );
            trace.integer_attribute("transfer.h2_window_wait_ms", integer(window.h2_wait_ms));
            trace.integer_attribute(
                "transfer.h2_window_wait_count",
                integer(window.h2_wait_count),
            );
            trace.double_attribute("transfer.h2_window_wait_ratio", window.h2_wait_ratio());
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
        let now = match &result {
            Poll::Ready(Some(Ok(frame)))
                if frame.data_ref().is_some_and(|bytes| !bytes.is_empty()) =>
            {
                Some(self.state.now_ms())
            }
            _ => None,
        };
        self.state.request_wait.observe(result.is_pending(), || {
            now.unwrap_or_else(|| self.state.now_ms())
        });
        if let (Some(now), Poll::Ready(Some(Ok(frame)))) = (now, &result) {
            if let Some(bytes) = frame.data_ref() {
                self.state.progress_at(true, bytes.len(), now);
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
        let this = self.as_mut().get_mut();
        if this.guard.registration.is_none() {
            return Poll::Ready(None);
        }
        let result = Pin::new(&mut this.inner).poll_frame(cx);
        let state = this.guard.progress();
        let now = match &result {
            Poll::Ready(Some(Ok(frame)))
                if frame.data_ref().is_some_and(|bytes| !bytes.is_empty()) =>
            {
                Some(state.now_ms())
            }
            _ => None,
        };
        state.response_wait.observe(result.is_pending(), || {
            now.unwrap_or_else(|| state.now_ms())
        });
        match &result {
            Poll::Pending => {}
            Poll::Ready(Some(Ok(frame))) => {
                if let (Some(now), Some(bytes)) = (now, frame.data_ref()) {
                    state.progress_at(false, bytes.len(), now);
                }
                if this.inner.is_end_stream() {
                    this.guard.finish("complete");
                }
            }
            Poll::Ready(None) => {
                this.guard.finish("complete");
            }
            Poll::Ready(Some(Err(_))) => {
                this.guard.finish("response_body_error");
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
