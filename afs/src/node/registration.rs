//! Meta registration of this Node: the bounded first registration made before
//! any mount or local API exists, and `MetaHeartbeat` (T2), which keeps the
//! lease alive while the Node serves.

#[cfg(feature = "dfs")]
use super::read;
use super::{NodeReadiness, rpc};
use crate::runtime::{Services, cancelled};
#[cfg(feature = "dfs")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "dfs")]
use std::time::Instant;
use std::{future::Future, sync::Arc, time::Duration};

const INITIAL_REGISTRATION_MIN_BUDGET: Duration = Duration::from_secs(10);
const INITIAL_REGISTRATION_MAX_BUDGET: Duration = Duration::from_secs(60);
const INITIAL_REGISTRATION_FIRST_DELAY: Duration = Duration::from_millis(100);
const INITIAL_REGISTRATION_MAX_DELAY: Duration = Duration::from_secs(2);

pub(super) fn initial_registration_budget(request_timeout: Duration) -> Duration {
    request_timeout
        .saturating_mul(3)
        .max(INITIAL_REGISTRATION_MIN_BUDGET)
        .min(INITIAL_REGISTRATION_MAX_BUDGET)
}

pub(super) async fn initial_shutdown_signal() {
    let Ok(mut terminate) =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    else {
        let _ = tokio::signal::ctrl_c().await;
        return;
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

pub(super) async fn retry_initial_registration<T, F, Fut, C>(
    mut attempt: F,
    cancellation: C,
    budget: Duration,
) -> afs_error::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = afs_error::Result<T>>,
    C: Future<Output = ()>,
{
    let deadline = tokio::time::Instant::now() + budget;
    let mut delay = INITIAL_REGISTRATION_FIRST_DELAY;
    let mut last_error = None;
    tokio::pin!(cancellation);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(last_error.unwrap_or_else(|| {
                afs_error::Error::coded(
                    afs_error::CLIENT_CONNECTION_UNAVAILABLE,
                    "initial Node registration retry budget expired",
                )
            }));
        }
        let result = tokio::select! {
            _ = &mut cancellation => {
                return Err(afs_error::Error::coded(
                    afs_error::IO_INTERRUPTED,
                    "initial Node registration cancelled",
                ));
            }
            result = tokio::time::timeout(remaining, attempt()) => match result {
                Ok(result) => result,
                Err(_) => {
                    return Err(last_error.unwrap_or_else(|| {
                        afs_error::Error::coded(
                            afs_error::CLIENT_CONNECTION_UNAVAILABLE,
                            "initial Node registration retry budget expired",
                        )
                    }));
                }
            }
        };
        match result {
            Ok(value) => return Ok(value),
            Err(error) if initial_registration_error_is_retryable(&error) => {
                afs_logging::warn!(
                    "node.initial_registration_retry";
                    "error" => error.to_string(),
                    "remaining_ms" => u64::try_from(
                        deadline
                            .saturating_duration_since(tokio::time::Instant::now())
                            .as_millis()
                    ).unwrap_or(u64::MAX)
                );
                last_error = Some(error);
                let sleep_for =
                    delay.min(deadline.saturating_duration_since(tokio::time::Instant::now()));
                tokio::select! {
                    _ = &mut cancellation => {
                        return Err(afs_error::Error::coded(
                            afs_error::IO_INTERRUPTED,
                            "initial Node registration cancelled",
                        ));
                    }
                    _ = tokio::time::sleep(sleep_for) => {}
                }
                delay = delay.saturating_mul(2).min(INITIAL_REGISTRATION_MAX_DELAY);
            }
            Err(error) => return Err(error),
        }
    }
}

/// Successful heartbeat cadence.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
/// Cadence after a retryable heartbeat failure.
const HEARTBEAT_RETRY_INTERVAL: Duration = Duration::from_secs(1);
/// A retryable failure stays tolerable only this long after the last success,
/// inside the Meta lease window.
const HEARTBEAT_FAIL_CLOSED_AFTER: Duration = Duration::from_secs(25);

/// T2 `MetaHeartbeat`: re-registers the Node with Meta every
/// [`HEARTBEAT_INTERVAL`], every [`HEARTBEAT_RETRY_INTERVAL`] after a
/// retryable failure.
///
/// Fatal class: a changed lease epoch, a non-retryable error, a refresh of the
/// running DFS read authority that fails while serving, or no success within
/// [`HEARTBEAT_FAIL_CLOSED_AFTER`] stops the read authority and returns `Err`,
/// which shuts the whole Node down. A retryable error inside that window is
/// logged (`node.meta_heartbeat_retry`) and retried. Stop: `services.stop`
/// ends it with `Ok`, checked between heartbeats; a late refresh during normal
/// shutdown is not a failure (`refresh_running_read_authority`).
pub(super) struct MetaHeartbeat {
    pub(super) endpoint: String,
    pub(super) descriptor: afs_protocol::meta::NodeDescriptor,
    pub(super) timeout: Duration,
    pub(super) tls: afs_transport::grpc::TlsConfig,
    pub(super) readiness: Arc<NodeReadiness>,
    pub(super) registered_node_epoch: u64,
    #[cfg(feature = "dfs")]
    pub(super) read_authority: Option<Arc<read::engine::LocalReadAuthority>>,
    #[cfg(feature = "dfs")]
    pub(super) shutting_down: Arc<AtomicBool>,
}

impl MetaHeartbeat {
    /// Start the heartbeat as one service; it stops with `services.stop`.
    pub(super) fn start(self, services: &mut Services) {
        let Self {
            endpoint,
            descriptor,
            timeout,
            tls,
            readiness,
            registered_node_epoch,
            #[cfg(feature = "dfs")]
            read_authority,
            #[cfg(feature = "dfs")]
            shutting_down,
        } = self;
        let stop = services.stop.subscribe();
        services.spawn(async move {
            // The Meta lease lasts longer than one refresh interval. A brief
            // Meta restart must not tear down the FUSE mount and all open FDs
            // just because one heartbeat raced the restart. Retry within the
            // lease window; fail closed if control cannot be restored in time.
            let mut last_success = tokio::time::Instant::now();
            let mut next_delay = HEARTBEAT_INTERVAL;
            let shutdown = cancelled(stop);
            tokio::pin!(shutdown);
            loop {
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    _ = tokio::time::sleep(next_delay) => {
                        match rpc::meta::register_node_with_readiness(
                            &endpoint,
                            descriptor.clone(),
                            timeout,
                            tls.clone(),
                        )
                        .await
                        {
                            Ok(registration) => {
                                if registration.lease_epoch != registered_node_epoch {
                                    #[cfg(feature = "dfs")]
                                    if let Some(authority) = &read_authority {
                                        authority.stop();
                                    }
                                    readiness.note_registration_failure(
                                        "Node registration epoch changed; the running authority must stop",
                                    );
                                    return Err(afs_error::Error::coded(
                                        afs_error::IO_PERMISSION_DENIED,
                                        "Node registration epoch changed; the running authority must stop",
                                    ).into());
                                }
                                #[cfg(feature = "dfs")]
                                if let Some(authority) = &read_authority
                                    && let Err(error) = refresh_running_read_authority(
                                        authority,
                                        registration.lease_epoch,
                                        registration.session_deadline,
                                        &shutting_down,
                                    ) {
                                    authority.stop();
                                    return Err(error.into());
                                }
                                readiness.note_registration_capability(&registration);
                                last_success = tokio::time::Instant::now();
                                next_delay = HEARTBEAT_INTERVAL;
                            }
                            Err(error) => {
                                readiness.note_registration_failure(error.to_string());
                                if !heartbeat_error_is_retryable(&error)
                                    || last_success.elapsed() >= HEARTBEAT_FAIL_CLOSED_AFTER
                                {
                                    #[cfg(feature = "dfs")]
                                    if let Some(authority) = &read_authority {
                                        authority.stop();
                                    }
                                    return Err(error.into());
                                }
                                afs_logging::warn!("node.meta_heartbeat_retry"; "error" => error.to_string());
                                next_delay = HEARTBEAT_RETRY_INTERVAL;
                            }
                        }
                    }
                }
            }
        });
    }
}

fn heartbeat_error_is_retryable(error: &afs_error::Error) -> bool {
    !matches!(
        error.kind(),
        afs_error::ErrorKind::InvalidArgument
            | afs_error::ErrorKind::PermissionDenied
            | afs_error::ErrorKind::Unauthenticated
    )
}

fn initial_registration_error_is_retryable(error: &afs_error::Error) -> bool {
    matches!(
        error.kind(),
        afs_error::ErrorKind::Unavailable
            | afs_error::ErrorKind::DeadlineExceeded
            | afs_error::ErrorKind::ResourceExhausted
            | afs_error::ErrorKind::Aborted
    )
}

#[cfg(feature = "dfs")]
fn refresh_running_read_authority(
    authority: &read::engine::LocalReadAuthority,
    epoch: u64,
    deadline: Instant,
    shutting_down: &AtomicBool,
) -> afs_error::Result<()> {
    match authority.refresh(epoch, deadline) {
        // SIGTERM closes read admission immediately. A heartbeat already in
        // flight may return after that stop; it cannot reopen authority and
        // must not turn a normal shutdown into a service failure. Epoch/expiry
        // failures while serving remain fatal.
        Err(_) if shutting_down.load(Ordering::Acquire) => Ok(()),
        result => result,
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "dfs")]
    #[test]
    fn registration_refresh_preserves_expired_and_changed_epoch_fail_closed() {
        use super::read::engine::LocalReadAuthority;
        use std::time::Instant;

        let now = Instant::now();
        let expired = LocalReadAuthority::new(7, now);
        assert!(expired.refresh(7, now + Duration::from_secs(30)).is_err());
        assert!(expired.refresh(7, now + Duration::from_secs(30)).is_err());

        let current = LocalReadAuthority::new(7, now + Duration::from_secs(30));
        assert!(current.refresh(8, now + Duration::from_secs(30)).is_err());
        assert!(current.refresh(7, now + Duration::from_secs(30)).is_err());
    }

    use std::time::Duration;

    #[test]
    fn heartbeat_retries_transport_failure_but_stops_expired_session() {
        assert!(super::heartbeat_error_is_retryable(
            &afs_error::Error::coded(
                afs_error::CLIENT_CONNECTION_UNAVAILABLE,
                "temporary transport outage",
            )
        ));
        assert!(!super::heartbeat_error_is_retryable(
            &afs_error::Error::coded(
                afs_error::CLIENT_ARGUMENT_INVALID,
                "expired node session cannot be renewed; start a new session_id",
            )
        ));
        assert!(!super::heartbeat_error_is_retryable(
            &afs_error::Error::coded(
                afs_error::IO_PERMISSION_DENIED,
                "registration authority rejected",
            )
        ));
    }

    #[tokio::test]
    async fn initial_registration_recovers_within_bounded_retry_window() {
        let mut attempts = 0;
        let value = super::retry_initial_registration(
            || {
                attempts += 1;
                let attempt = attempts;
                async move {
                    if attempt < 3 {
                        Err(afs_error::Error::coded(
                            afs_error::CLIENT_CONNECTION_UNAVAILABLE,
                            "Meta is still starting",
                        ))
                    } else {
                        Ok(41)
                    }
                }
            },
            std::future::pending(),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        assert_eq!(value, 41);
        assert_eq!(attempts, 3);
    }

    #[tokio::test]
    async fn initial_registration_returns_last_retryable_error_when_budget_expires() {
        let mut attempts = 0;
        let error = super::retry_initial_registration::<(), _, _, _>(
            || {
                attempts += 1;
                async {
                    Err(afs_error::Error::coded(
                        afs_error::CLIENT_CONNECTION_UNAVAILABLE,
                        "Meta is still starting",
                    ))
                }
            },
            std::future::pending(),
            Duration::from_millis(50),
        )
        .await
        .unwrap_err();

        assert_eq!(error.code(), afs_error::CLIENT_CONNECTION_UNAVAILABLE);
        assert_eq!(error.message(), "Meta is still starting");
        assert!(attempts >= 1);
    }

    #[tokio::test]
    async fn initial_registration_stops_on_non_retryable_identity_failure() {
        let mut attempts = 0;
        let error = super::retry_initial_registration::<(), _, _, _>(
            || {
                attempts += 1;
                async {
                    Err(afs_error::Error::coded(
                        afs_error::IO_PERMISSION_DENIED,
                        "certificate belongs to another Node",
                    ))
                }
            },
            std::future::pending(),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), afs_error::ErrorKind::PermissionDenied);
        assert_eq!(attempts, 1);
    }

    #[tokio::test]
    async fn initial_registration_can_be_cancelled_without_waiting_for_budget() {
        let (cancel, cancelled) = tokio::sync::oneshot::channel::<()>();
        cancel.send(()).unwrap();
        let error = super::retry_initial_registration::<(), _, _, _>(
            || async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok(())
            },
            async move {
                let _ = cancelled.await;
            },
            Duration::from_secs(60),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), afs_error::ErrorKind::Cancelled);
        assert!(error.to_string().contains("cancelled"));
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn normal_shutdown_late_heartbeat_does_not_fail_or_reopen_authority() {
        use std::{
            sync::atomic::{AtomicBool, Ordering},
            time::Instant,
        };
        let authority = super::read::engine::LocalReadAuthority::new(
            1,
            Instant::now() + Duration::from_secs(30),
        );
        let stopping = AtomicBool::new(true);
        authority.stop();
        super::refresh_running_read_authority(
            &authority,
            1,
            Instant::now() + Duration::from_secs(30),
            &stopping,
        )
        .unwrap();
        assert!(
            authority
                .refresh(1, Instant::now() + Duration::from_secs(30))
                .is_err(),
            "late heartbeat must not reopen stopped authority"
        );
        stopping.store(false, Ordering::Release);
        assert!(
            super::refresh_running_read_authority(
                &authority,
                1,
                Instant::now() + Duration::from_secs(30),
                &stopping
            )
            .is_err(),
            "stopped authority outside normal shutdown must fail closed"
        );
    }
}
