//! Ambient deadline for blocking calls to Meta and to remote DFS owners.
//!
//! Most calls run under the client's configured timeout. A maintenance loop
//! that spends a shared budget wraps one call in [`within`] or
//! [`within_timeout`]; the gRPC client then takes the smaller of the remaining
//! scope and its configured timeout via [`bounded`]. A configured timeout is
//! therefore always an upper bound, and a scope can only shorten a call.
//!
//! The scope belongs to the calling thread and **does not cross threads**:
//! `spawn_blocking`, `std::thread::spawn` and spawned tasks start without it.
//! The gRPC clients read it on the calling thread, before `block_on`, so wrap
//! the blocking call itself. Async work uses `read_pipeline::ReadScope` instead.
use std::{
    cell::Cell,
    time::{Duration, Instant},
};

thread_local! {
    static CALL_DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) };
}

/// Restores the enclosing scope, including when `work` panics.
struct RestoreOuter(Option<Instant>);

impl Drop for RestoreOuter {
    fn drop(&mut self) {
        CALL_DEADLINE.set(self.0);
    }
}

/// Run `work` under `deadline`. Nested scopes keep the earliest deadline.
pub(crate) fn within<T>(deadline: Instant, work: impl FnOnce() -> T) -> T {
    let outer = CALL_DEADLINE.get();
    let _restore = RestoreOuter(outer);
    CALL_DEADLINE.set(Some(outer.map_or(deadline, |outer| outer.min(deadline))));
    work()
}

/// Run `work` under a deadline `timeout` from now.
pub(crate) fn within_timeout<T>(timeout: Duration, work: impl FnOnce() -> T) -> T {
    match Instant::now().checked_add(timeout) {
        Some(deadline) => within(deadline, work),
        None => work(),
    }
}

/// Time left in the current scope, or `None` outside any scope.
pub(crate) fn remaining() -> Option<Duration> {
    CALL_DEADLINE
        .get()
        .map(|deadline| deadline.saturating_duration_since(Instant::now()))
}

/// The timeout for a call whose client is configured with `configured`.
pub(crate) fn bounded(configured: Duration) -> Duration {
    remaining().map_or(configured, |remaining| remaining.min(configured))
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONFIGURED: Duration = Duration::from_secs(3600);

    #[test]
    fn absent_scope_uses_the_configured_timeout() {
        assert_eq!(remaining(), None);
        assert_eq!(bounded(CONFIGURED), CONFIGURED);
    }

    #[test]
    fn scope_shortens_but_never_extends_the_configured_timeout() {
        within_timeout(Duration::from_secs(60), || {
            let bound = bounded(CONFIGURED);
            assert!(bound <= Duration::from_secs(60));
            assert!(bound > Duration::from_secs(30));
            assert_eq!(bounded(Duration::from_millis(5)), Duration::from_millis(5));
        });
    }

    #[test]
    fn nested_scopes_take_the_earliest_deadline_and_restore_the_outer_one() {
        let soon = Instant::now() + Duration::from_secs(60);
        let later = soon + Duration::from_secs(600);
        within(soon, || {
            within(later, || {
                assert!(bounded(CONFIGURED) <= Duration::from_secs(60))
            });
            within(soon - Duration::from_secs(30), || {
                assert!(bounded(CONFIGURED) <= Duration::from_secs(30));
            });
            assert!(bounded(CONFIGURED) > Duration::from_secs(30));
        });
        assert_eq!(remaining(), None);
    }

    #[test]
    fn expired_scope_leaves_no_time() {
        within_timeout(Duration::ZERO, || {
            assert_eq!(bounded(CONFIGURED), Duration::ZERO)
        });
    }

    #[test]
    fn scope_ends_when_work_panics() {
        let panicked = std::panic::catch_unwind(|| {
            within_timeout(Duration::from_secs(1), || panic!("injected"));
        });
        assert!(panicked.is_err());
        assert_eq!(remaining(), None);
    }

    #[test]
    fn scope_does_not_cross_threads() {
        within_timeout(Duration::from_millis(1), || {
            let spawned = std::thread::spawn(|| (remaining(), bounded(CONFIGURED)))
                .join()
                .unwrap();
            assert_eq!(spawned, (None, CONFIGURED));
        });
    }
}
