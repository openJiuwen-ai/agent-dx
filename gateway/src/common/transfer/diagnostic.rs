//! Slow-only snapshots leave the transfer locks before entering the log sink.
use super::*;
use std::sync::mpsc::{sync_channel, SyncSender};

const QUEUE_CAPACITY: usize = 64;

pub(super) fn start_worker() -> Option<SyncSender<Diagnostic>> {
    let (sender, receiver) = sync_channel::<Diagnostic>(QUEUE_CAPACITY);
    std::thread::Builder::new()
        .name("adx-transfer-logs".into())
        .spawn(move || {
            while let Ok(diagnostic) = receiver.recv() {
                diagnostic.emit();
            }
        })
        .ok()
        .map(|_| sender)
}

pub(super) struct Diagnostic {
    dispatch: tracing::Dispatch,
    identity: TransferIdentity,
    event: &'static str,
    outcome: &'static str,
    elapsed_ms: u64,
    headers_ms: u64,
    request_bytes: u64,
    response_bytes: u64,
    last_progress_age_ms: u64,
    slow_reason: &'static str,
    request_wait_ms: u64,
    response_wait_ms: u64,
    h2: H2Snapshot,
    h2_tunnel_id: String,
    h2_stream_id: u32,
    window: TransferWindow,
    frontend: (u64, u64, u64, u64),
}
impl Diagnostic {
    pub(super) fn capture(
        state: &TransferProgress,
        event: &'static str,
        outcome: &'static str,
    ) -> Self {
        let now = state.now_ms();
        let (h2_tunnel_id, h2_stream_id) = state.h2_identity();
        let frontend = state.frontend.as_ref().map_or((0, 0, 0, 0), |connection| {
            let (wait, pending) = connection.write_wait.snapshot(connection.now_ms());
            (
                connection.id,
                connection.written.load(Ordering::Relaxed),
                wait,
                pending,
            )
        });
        Self {
            dispatch: tracing::dispatcher::get_default(Clone::clone),
            identity: state.identity.clone(),
            event,
            outcome,
            elapsed_ms: now,
            headers_ms: state.headers_ms.load(Ordering::Relaxed),
            request_bytes: state.request_bytes.load(Ordering::Relaxed),
            response_bytes: state.response_bytes.load(Ordering::Relaxed),
            last_progress_age_ms: state.progress_age_ms(),
            slow_reason: state.reason(),
            request_wait_ms: state.request_wait.snapshot(now).0,
            response_wait_ms: state.response_wait.snapshot(now).0,
            h2: state.h2_snapshot(),
            h2_tunnel_id,
            h2_stream_id,
            window: state.window(),
            frontend,
        }
    }
    fn emit(self) {
        tracing::dispatcher::with_default(&self.dispatch, || {
            tracing::warn!(target:"adx_transfer",event=self.event,outcome=self.outcome,
                component=self.identity.component,direction=self.identity.direction,
                request_id=%self.identity.request_id,trace_id=%self.identity.trace_id,span_id=%self.identity.span_id,
                elapsed_ms=self.elapsed_ms,headers_ms=self.headers_ms,
                request_bytes=self.request_bytes,response_bytes=self.response_bytes,
                last_progress_age_ms=self.last_progress_age_ms,slow_reason=self.slow_reason,
                request_wait_ms=self.request_wait_ms,response_wait_ms=self.response_wait_ms,
                h2_capacity_wait_ms=self.h2.wait_ms,h2_capacity_wait_count=self.h2.wait_count,
                h2_capacity_wait_ratio=ratio(self.h2.wait_ms,self.elapsed_ms),
                h2_capacity_pending_ms=self.h2.pending_ms,h2_enqueued_bytes=self.h2.enqueued_bytes,
                h2_tunnel_id=%self.h2_tunnel_id,h2_stream_id=self.h2_stream_id,
                window_ms=self.window.duration_ms,window_end_ms=self.window.end_ms,
                window_request_bytes=self.window.request_bytes,window_response_bytes=self.window.response_bytes,
                h2_window_wait_ms=self.window.h2_wait_ms,h2_window_wait_count=self.window.h2_wait_count,
                h2_window_wait_ratio=self.window.h2_wait_ratio(),h2_window_enqueued_bytes=self.window.h2_enqueued_bytes,
                frontend_connection_id=self.frontend.0,frontend_connection_written_bytes=self.frontend.1,
                frontend_write_wait_ms=self.frontend.2,frontend_write_pending_ms=self.frontend.3,
                "Slow transfer diagnostic");
        });
    }
}
