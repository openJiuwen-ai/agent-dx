//! Coordinator readiness: the process role and whether it is ready to
//! serve, exposed at `GET /readyz` for deployment probes and role
//! observation.
//!
//! Readiness only distinguishes "initialization not complete". Within a
//! role it never flips back on transient dependency failures: a standby
//! whose synchronization goes stale after its first success stays ready —
//! it is still a promotable replica, and probing a Redis blip would drop
//! every replica at once. `Draining` (handover) and role loss are not
//! transient states: a draining or lost primary is not ready, so the
//! orchestrator replaces it.
use bytes::Bytes;
use http_body_util::Full;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Connecting to storage; the role is not yet acquired.
    Preparing,
    /// Read-only standby. `synced` latches after the first successful
    /// read-only synchronization and never unlatches: later sync failures
    /// mark the view stale but the replica remains promotable.
    Standby { synced: bool },
    /// Primary with a bound epoch.
    Primary,
    /// Handing over: new work is rejected while in-flight work finishes.
    Draining,
}

impl Role {
    /// Whether a probe should consider this process ready.
    pub fn ready(&self) -> bool {
        match self {
            Role::Preparing => false,
            Role::Standby { synced } => *synced,
            Role::Primary => true,
            Role::Draining => false,
        }
    }
    pub fn label(&self) -> &'static str {
        match self {
            Role::Preparing => "preparing",
            Role::Standby { .. } => "standby",
            Role::Primary => "primary",
            Role::Draining => "draining",
        }
    }
}

/// Shared role state, updated by the process lifecycle and read by the
/// readiness endpoint.
#[derive(Clone)]
pub struct Readiness(watch::Sender<Role>);

impl Readiness {
    pub fn new() -> Self {
        Self(watch::channel(Role::Preparing).0)
    }
    pub fn get(&self) -> Role {
        *self.0.borrow()
    }
    /// Enter or update the standby role. A successful synchronization
    /// latches readiness and never unlatches; a failed refresh of an
    /// already-synced standby changes nothing.
    pub fn standby(&self, synced: bool) {
        self.0.send_if_modified(|role| {
            if matches!(role, Role::Standby { synced: true }) && !synced {
                return false;
            }
            if matches!(role, Role::Primary | Role::Draining) {
                return false;
            }
            if *role == (Role::Standby { synced }) {
                return false;
            }
            *role = Role::Standby { synced };
            true
        });
    }
    /// Promote to primary (after `begin` and the initial advertisement).
    pub fn promote(&self) {
        self.0.send_modify(|role| {
            if !matches!(role, Role::Primary | Role::Draining) {
                *role = Role::Primary;
            }
        });
    }
    /// Start the handover drain: not ready while in-flight work finishes.
    pub fn drain(&self) {
        self.0.send_replace(Role::Draining);
    }
}

impl Default for Readiness {
    fn default() -> Self {
        Self::new()
    }
}

/// Serve `GET /readyz`: 200 with the role once ready, 503 while not.
/// `GET /healthz` answers 200 as long as the process is serving probes, so
/// a graceful handover drain is not mistaken for a dead replica.
pub async fn serve(listener: TcpListener, readiness: Readiness) -> std::io::Result<()> {
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(accepted) => accepted,
            // Transient accept errors (e.g. ECONNABORTED) must not kill the
            // probe service: this listener is the orchestrator's only
            // signal for the role.
            Err(error) => {
                adx_observability::warn!("Coordinator readiness accept failed: {error}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let readiness = readiness.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |request| {
                let readiness = readiness.clone();
                async move {
                    if request.method() != hyper::Method::GET {
                        return Ok::<_, std::convert::Infallible>(
                            hyper::Response::builder()
                                .status(hyper::StatusCode::METHOD_NOT_ALLOWED)
                                .header("Content-Type", "application/json")
                                .body(Full::new(Bytes::from(
                                    r#"{"error":{"message":"method not allowed"}}"#.to_owned(),
                                )))
                                .expect("static readiness response is valid"),
                        );
                    }
                    let (status, body) = match request.uri().path() {
                        "/healthz" => (hyper::StatusCode::OK, r#"{"status":"ok"}"#.to_owned()),
                        "/readyz" => {
                            // Build the body directly: serde_json maps sort
                            // keys alphabetically, but the documented field
                            // order is role-first.
                            let role = readiness.get();
                            if role.ready() {
                                (
                                    hyper::StatusCode::OK,
                                    format!(r#"{{"role":"{}","ready":true}}"#, role.label()),
                                )
                            } else {
                                (
                                    hyper::StatusCode::SERVICE_UNAVAILABLE,
                                    format!(r#"{{"role":"{}","ready":false}}"#, role.label()),
                                )
                            }
                        }
                        _ => {
                            return Ok(hyper::Response::builder()
                                .status(hyper::StatusCode::NOT_FOUND)
                                .header("Content-Type", "application/json")
                                .body(Full::new(Bytes::from(
                                    r#"{"error":{"message":"not found"}}"#.to_owned(),
                                )))
                                .expect("static readiness response is valid"))
                        }
                    };
                    Ok::<_, std::convert::Infallible>(
                        hyper::Response::builder()
                            .status(status)
                            .header("Content-Type", "application/json")
                            .body(Full::new(Bytes::from(body)))
                            .expect("static readiness response is valid"),
                    )
                }
            });
            let connection = hyper::server::conn::http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(stream), service);
            let _ = tokio::time::timeout(Duration::from_secs(10), connection).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_only_distinguishes_initialization() {
        let readiness = Readiness::new();
        assert!(!readiness.get().ready());
        readiness.standby(false);
        assert!(!readiness.get().ready());
        readiness.standby(true);
        assert!(readiness.get().ready());
        // A stale refresh never un-readies a synced standby.
        readiness.standby(false);
        assert!(readiness.get().ready());
        readiness.promote();
        assert_eq!(readiness.get(), Role::Primary);
        // Late standby updates must not demote a primary.
        readiness.standby(true);
        assert_eq!(readiness.get(), Role::Primary);
        readiness.drain();
        assert_eq!(readiness.get(), Role::Draining);
        assert!(!readiness.get().ready());
    }
}
