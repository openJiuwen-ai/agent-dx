//! A request deadline shared by admission, demand wake, route delivery and forwarding.
use http::HeaderMap;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use std::future::Future;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;

pub const TIMEOUT_HEADER: &str = "x-adx-processing-timeout-ms";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_TIMEOUT_MS: u64 = 86_400_000;

#[derive(Debug, Clone)]
pub struct ProcessingDeadline {
    pub at: Instant,
    phase: Arc<AtomicU8>,
    timeout: Duration,
    upload_deadline: watch::Sender<Instant>,
}

impl Default for ProcessingDeadline {
    fn default() -> Self {
        Self::new(DEFAULT_TIMEOUT)
    }
}

impl ProcessingDeadline {
    pub fn new(timeout: Duration) -> Self {
        let at = Instant::now() + timeout;
        Self {
            at,
            phase: Arc::new(AtomicU8::new(0)),
            timeout,
            upload_deadline: watch::channel(at).0,
        }
    }

    pub fn from_headers(headers: &HeaderMap) -> Result<Self, &'static str> {
        let mut values = headers.get_all(TIMEOUT_HEADER).iter();
        let Some(value) = values.next() else {
            return Ok(Self::default());
        };
        if values.next().is_some() {
            return Err("processing timeout must occur once");
        }
        let milliseconds = value
            .to_str()
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n > 0 && *n <= MAX_TIMEOUT_MS)
            .ok_or("processing timeout must be 1 through 86400000 milliseconds")?;
        Ok(Self::new(Duration::from_millis(milliseconds)))
    }

    pub fn remaining(&self) -> Duration {
        self.at.saturating_duration_since(Instant::now())
    }
    pub fn waiting_resume(&self) {
        self.phase.store(1, Ordering::Release);
    }
    pub fn prepared(&self) {
        self.phase.store(0, Ordering::Release);
    }
    pub fn forwarded(&self) {
        self.phase.store(2, Ordering::Release);
    }
    pub fn is_waiting_resume(&self) -> bool {
        self.phase.load(Ordering::Acquire) == 1
    }
    pub fn is_forwarded(&self) -> bool {
        self.phase.load(Ordering::Acquire) == 2
    }

    pub(super) fn response_deadline(&self) -> Instant {
        *self.upload_deadline.borrow()
    }

    /// Upstream request-body progress renews the processing window. Admission
    /// still uses `at`; neither empty frames nor waiting for response headers
    /// renew this window.
    fn upload_progress(&self) {
        if self.is_forwarded() {
            self.upload_deadline
                .send_replace(Instant::now() + self.timeout);
        }
    }

    pub(super) async fn until_headers<F: Future>(
        &self,
        future: F,
    ) -> Result<F::Output, tokio::time::error::Elapsed> {
        tokio::pin!(future);
        loop {
            let deadline = self.response_deadline();
            match tokio::time::timeout_at(deadline, &mut future).await {
                Err(_) if self.response_deadline() > deadline => continue,
                result => return result,
            }
        }
    }
}

/// Wrap only at the forwarding boundary, preserving the original Incoming body
/// for requests that have not yet passed route and authorization admission.
pub(super) fn forwarding_request(
    request: http::Request<Incoming>,
) -> http::Request<super::server::ProxyBody> {
    let processing = request.extensions().get::<ProcessingDeadline>().cloned();
    request.map(|body| {
        body.map_frame(move |frame| {
            if frame.data_ref().is_some_and(|bytes| !bytes.is_empty()) {
                if let Some(processing) = &processing {
                    processing.upload_progress();
                }
            }
            frame
        })
        .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) })
        .boxed_unsync()
    })
}
