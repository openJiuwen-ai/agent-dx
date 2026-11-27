use adx_activator::{
    config::ActivatorConfig,
    reconciliation::{ReconcileSettings, Reconciliation},
    registration::ConditionSource,
    server,
    transport::HttpEnvironment,
};
use adx_agent_core::discovery::MemberCondition;
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, OnceLock,
    },
    time::Duration,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    #[serde(flatten)]
    activator: ActivatorConfig,
    listen: SocketAddr,
    environment_url: String,
    environment_api_key_envs: BTreeMap<String, String>,
    allow_plaintext_transport: bool,
    environment_ca_file: Option<String>,
    /// Budget for the SIGTERM exit: unregister retries plus the wait for
    /// already-accepted requests (each additionally bounded by its request
    /// deadline). Work still running when the budget expires is cut by the
    /// exit watchdog.
    #[serde(default = "shutdown_timeout_seconds")]
    shutdown_timeout_seconds: u64,
}
fn shutdown_timeout_seconds() -> u64 {
    15
}

/// Backoff between initialization attempts while dependencies (Redis, API
/// Server) are unreachable. The process keeps answering 503 probes during
/// this window, so the Deployment does not advance the batch, and recovers
/// without a restart once the dependency returns.
const INIT_RETRY_BACKOFF: Duration = Duration::from_secs(1);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut settings: Settings =
        serde_json::from_slice(&std::fs::read(std::env::var("ADX_ACTIVATOR_CONFIG")?)?)?;
    if !(1..=300).contains(&settings.shutdown_timeout_seconds) {
        return Err("shutdown_timeout_seconds must be in 1..=300".into());
    }
    if !settings.allow_plaintext_transport {
        return Err(
            "private listener requires explicit plaintext authorization behind a TLS service proxy"
                .into(),
        );
    }
    let timeout = settings.activator.request_timeout();
    let shutdown_timeout = Duration::from_secs(settings.shutdown_timeout_seconds);
    let ca = settings
        .environment_ca_file
        .map(std::fs::read)
        .transpose()?;
    let environment = Arc::new(HttpEnvironment::new(
        &settings.environment_url,
        settings
            .environment_api_key_envs
            .into_iter()
            .map(|(tenant, variable)| std::env::var(variable).map(|key| (tenant, key)))
            .collect::<Result<BTreeMap<_, _>, _>>()?,
        timeout,
        ca.as_deref(),
        settings.allow_plaintext_transport,
        settings.activator.egress_isolation.clone(),
    )?);
    // Bind before initializing so readiness probes are answerable (503)
    // while dependencies are still coming up: the listener being up proves
    // nothing about initialization.
    let listener = tokio::net::TcpListener::bind(settings.listen).await?;
    let activator_slot: server::SharedActivator = Arc::new(OnceLock::new());
    let draining = Arc::new(AtomicBool::new(false));
    // Member-record condition: `ready` requires the Activator slot to be
    // set — from that instant the request handlers can actually serve.
    // Registration alone must not claim ready: Gateway selection arriving
    // between the lease write and the slot would hit "initialization in
    // progress" rejections.
    let condition: ConditionSource = {
        let activator_slot = activator_slot.clone();
        let draining = draining.clone();
        Arc::new(move || MemberCondition {
            ready: activator_slot.get().is_some(),
            draining: draining.load(Ordering::Acquire),
        })
    };
    let reconciliation: Arc<std::sync::Mutex<Option<Reconciliation>>> =
        Arc::new(std::sync::Mutex::new(None));
    let app = server::router_with_readiness(
        activator_slot.clone(),
        draining.clone(),
        &std::env::var("ADX_ACTIVATOR_SERVICE_TOKEN")?,
        timeout,
    )?;
    // Initialization order: connect the store, verify the environment
    // management endpoint, build the Activator, then start alias
    // reconciliation with its membership lease. Dependency outages retry
    // with the process alive (probe stays 503, the Deployment does not
    // advance this batch); configuration errors exit non-zero. The
    // Activator is published — and the readiness probe turns 204 — only
    // after the lease has been registered, so an uninitialized replica is
    // never selected.
    // Process-stable lease identity: initialization retries renew their
    // own lease instead of colliding with it as a (fatal) Conflict.
    if let Some(registration) = settings.activator.registration.as_mut() {
        registration.incarnation = Some(uuid::Uuid::new_v4().to_string());
    }
    let initialization = {
        // The shutdown path below keeps the originals of these two; the
        // initialization task works on its own clones. Everything else it
        // needs is captured by the async block directly.
        let reconciliation = reconciliation.clone();
        let draining = draining.clone();
        let activator_config = settings.activator;
        let allow_plaintext = settings.allow_plaintext_transport;
        tokio::spawn(async move {
            loop {
                // A SIGTERM that arrived while a dependency was failing must
                // not start another registration round; a lease this attempt
                // already wrote is released by its own draining handling
                // below or by the heartbeat task's draining flip.
                if draining.load(Ordering::Acquire) {
                    return;
                }
                if let Err(error) = environment.reachable().await {
                    retry("API Server reachability", error).await;
                    continue;
                }
                // ActivatorConfig::connect bundles the store, the Activator
                // and the image-registry resolver; configuration errors are
                // fatal while dependency outages retry with the latch held.
                let connected = activator_config.connect(environment.clone()).await;
                let (activator, store) = match connected {
                    Ok(connected) => connected,
                    Err(error) if matches!(error, adx_activator::Error::Invalid(_)) => {
                        fatal(format!("Activator configuration rejected: {error}"));
                    }
                    Err(error) => {
                        retry("Activator connect", error).await;
                        continue;
                    }
                };
                // `ActivatorConfig::connect` returns the Activator behind an
                // Arc already. Upstream starts snapshot preparations
                // immediately after construction; the latch keeps that
                // immediate start.
                activator.start_snapshot_preparations();
                // The Activator itself is now functional, but the member
                // record stays not-ready until the slot below is set: the
                // lease heartbeat turns it ready within one poll once this
                // replica can actually serve requests.
                let registered = match activator_config.registration.clone() {
                    Some(config) => {
                        let registry = match adx_agent_store::discovery::RedisRegistry::new(
                            &activator_config.redis_url,
                            &activator_config.namespace,
                            Duration::from_secs(3),
                        ) {
                            Ok(registry) => registry,
                            Err(error) => {
                                fatal(format!("Activator registry rejected: {error}"));
                            }
                        };
                        let started = Reconciliation::start(
                            activator.clone(),
                            store.clone(),
                            registry,
                            config,
                            ReconcileSettings::default(),
                            allow_plaintext,
                            condition.clone(),
                        )
                        .await;
                        match started {
                            Ok(reconciliation) => Some(reconciliation),
                            Err(error) if registration_fatal(&error) => {
                                fatal(format!("Activator registration rejected: {error}"));
                            }
                            Err(error) => {
                                retry("Activator registration", error).await;
                                continue;
                            }
                        }
                    }
                    None => None,
                };
                // A SIGTERM that arrived while initialization was still
                // retrying must not leave a registered member behind: remove
                // the lease instead of publishing it. The re-check happens
                // under the registration lock — the same lock the shutdown
                // sweep takes after setting draining — so a lease is either
                // stored before a sweep (and swept by it) or never stored;
                // no SIGTERM can land between the check and the store.
                let mut slot = reconciliation
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if draining.load(Ordering::Acquire) {
                    drop(slot);
                    if let Some(registered) = registered {
                        if let Err(error) = registered.shutdown(shutdown_timeout).await {
                            tracing::warn!(
                                %error,
                                "Activator reconciliation shutdown failed; lease will expire"
                            );
                        }
                    }
                    return;
                }
                *slot = registered;
                drop(slot);
                let _ = activator_slot.set(activator);
                tracing::info!("Activator initialized and registered for selection");
                return;
            }
        })
    };
    let shutdown_reconciliation = reconciliation.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            #[cfg(unix)]
            {
                let mut signal =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("SIGTERM handler");
                tokio::select! { _=signal.recv()=>(), _=tokio::signal::ctrl_c()=>() }
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
            }
            let started = tokio::time::Instant::now();
            // Arm the watchdog FIRST: a hang in the unregister retries below
            // must not prevent it from ever firing. The bound is the
            // overall exit budget (unregister budget + one request deadline
            // + slack); deployments must grant a termination grace period
            // of at least this long for the graceful path to complete.
            tokio::spawn(async move {
                let bound = shutdown_timeout + timeout + Duration::from_secs(1);
                tokio::time::sleep_until(started + bound).await;
                tracing::warn!(
                    "Activator shutdown budget exhausted; exiting with in-flight work cut"
                );
                std::process::exit(0);
            });
            // Draining comes first: new work is rejected explicitly (callers
            // retry another member) while already-accepted requests keep
            // running. The readiness probe follows the draining flag.
            draining.store(true, Ordering::Release);
            // Abort alias reconciliation, stop lease renewal, then unregister
            // and announce the removal, retried within the shutdown budget;
            // on final failure the lease expiry removes the member. A lease
            // the initialization task stored after this take is caught by
            // the final sweep after the accept loop ends.
            let registered = shutdown_reconciliation
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take();
            if let Some(reconciliation) = registered {
                if let Err(error) = reconciliation.shutdown(shutdown_timeout).await {
                    tracing::warn!(
                        %error,
                        "Activator reconciliation shutdown failed; lease will expire"
                    );
                }
            }
            // Returning stops the accept loop and closes idle connections;
            // in-flight requests finish, each bounded by its request
            // deadline.
        })
        .await?;
    // The initialization task exits promptly under draining (its loop and
    // its registration lock both check the flag); awaiting it lets a lease
    // it still holds be released by its own draining handling instead of
    // being cut mid-start by the runtime teardown.
    let _ = initialization.await;
    // Final sweep: a lease the initialization task stored between the take
    // above and this point (the accept loop kept serving while the
    // unregister retried) is removed here.
    let final_sweep = reconciliation
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take();
    if let Some(reconciliation) = final_sweep {
        if let Err(error) = reconciliation.shutdown(shutdown_timeout).await {
            tracing::warn!(
                %error,
                "Activator reconciliation shutdown failed; lease will expire"
            );
        }
    }
    Ok(())
}

/// Retry an initialization step whose dependency is unavailable: keep the
/// process (and its 503 probe) alive and try again after the backoff.
async fn retry(step: &str, error: impl std::fmt::Display) {
    tracing::warn!(%error, step, "initialization dependency unavailable; retrying");
    tokio::time::sleep(INIT_RETRY_BACKOFF).await;
}

/// Registration failures no retry can fix (configuration, duplicate live
/// instance identity): the Deployment must not keep this batch.
fn registration_fatal(error: &adx_agent_core::error::Error) -> bool {
    matches!(
        error,
        adx_agent_core::error::Error::Invalid(_) | adx_agent_core::error::Error::Conflict(_)
    )
}

/// Initialization failed for a non-transient reason (configuration, identity
/// conflict): exit non-zero so the Deployment does not keep the batch.
fn fatal(message: String) -> ! {
    tracing::error!(%message, "Activator initialization failed; exiting");
    std::process::exit(1)
}

#[cfg(test)]
mod tests {
    use super::Settings;
    use serde_json::{json, Value};

    #[test]
    fn standalone_settings_read_the_shared_config_at_the_root() {
        let mut config: Value =
            serde_json::from_str(include_str!("../examples/discovery.json")).unwrap();
        let settings: Settings = serde_json::from_value(config.clone()).unwrap();
        assert!(settings.activator.egress_isolation.is_some());
        assert!(settings.activator.registration.is_some());

        config["request_timeout_second"] = json!(10);
        assert!(serde_json::from_value::<Settings>(config).is_err());
    }
}
