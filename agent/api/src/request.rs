//! One caller-owned deadline for management or target activation, never for a live stream.
use crate::{Error, Result};
use adx_agent_core::transport::RequestProgress;
use std::{future::Future, time::Duration};
use tokio::time::Instant;

pub struct RequestContext {
    deadline: Instant,
    progress: RequestProgress,
}
impl RequestContext {
    /// Construct a deadline from a validated entrypoint budget.
    ///
    /// # Panics
    /// Panics if the budget exceeds the monotonic clock's range. Process configuration
    /// must reject such values before accepting requests.
    pub fn new(timeout: Duration) -> Self {
        Self {
            deadline: Instant::now() + timeout,
            progress: RequestProgress::default(),
        }
    }
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    /// Convert the unspent monotonic budget for a downstream service call.
    pub fn deadline_unix_ms(&self) -> u64 {
        adx_agent_core::transport::capped_deadline(
            None,
            self.deadline.saturating_duration_since(Instant::now()),
        )
    }
    pub fn start_write(&self) {
        self.progress.start_write();
    }
    /// Execute within the original deadline. A completed typed result is returned unchanged.
    /// Timeout is Unavailable for reads, OutcomeUnknown after a possible write. An expired
    /// context rejects a new attempt without polling it, even when an earlier attempt wrote.
    pub async fn run<T>(&self, operation: impl Future<Output = Result<T>>) -> Result<T> {
        if Instant::now() >= self.deadline {
            return Err(Error::Unavailable("request deadline expired".into()));
        }
        tokio::time::timeout_at(self.deadline, operation)
            .await
            .unwrap_or_else(|_| {
                Err(if self.progress.may_have_written() {
                    Error::OutcomeUnknown(
                        "operation timed out; inspect the original identity".into(),
                    )
                } else {
                    Error::Unavailable("read or request admission timed out".into())
                })
            })
    }
}
