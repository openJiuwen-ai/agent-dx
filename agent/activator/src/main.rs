use adx_activator::{
    reconciliation::{ReconcileSettings, Reconciliation},
    registration::{ConditionSource, RegistrationConfig},
    server,
    transport::HttpEnvironment,
    Activator, CacheSettings,
};
use adx_agent_core::discovery::MemberCondition;
use adx_agent_store::{AgentState, RedisRepository};
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
    listen: SocketAddr,
    redis_url: String,
    namespace: String,
    environment_url: String,
    environment_api_key_envs: BTreeMap<String, String>,
    sandbox_network: adx_agent_core::network::SandboxNetworkPolicy,
    allow_plaintext_transport: bool,
    environment_ca_file: Option<String>,
    request_timeout_seconds: u64,
    /// Budget for the SIGTERM exit: unregister retries plus the wait for
    /// already-accepted requests (each additionally bounded by its request
    /// deadline). Work still running when the budget expires is cut by the
    /// exit watchdog.
    #[serde(default = "shutdown_timeout_seconds")]
    shutdown_timeout_seconds: u64,
    #[serde(default)]
    binding_cache: CacheSettings,
    registration: Option<RegistrationConfig>,
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
    let settings: Settings =
        serde_json::from_slice(&std::fs::read(std::env::var("ADX_ACTIVATOR_CONFIG")?)?)?;
    if !settings.allow_plaintext_transport {
        return Err(
            "private listener requires explicit plaintext authorization behind a TLS service proxy"
                .into(),
        );
    }
    let timeout = Duration::from_secs(settings.request_timeout_seconds);
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
        settings.sandbox_network,
    )?);
    // Bind before initializing so readiness probes are answerable (503)
    // while dependencies are still coming up: the listener being up proves
    // nothing about initialization.
    let listener = tokio::net::TcpListener::bind(settings.listen).await?;
    let activator_slot: server::SharedActivator = Arc::new(OnceLock::new());
    let draining = Arc::new(AtomicBool::new(false));
    // Member-record condition: `ready` flips once the Activator itself is
    // constructed (its dependencies answered); the slot below additionally
    // requires registration and gates the probe and request handlers.
    let member_ready = Arc::new(AtomicBool::new(false));
    let condition: ConditionSource = {
        let member_ready = member_ready.clone();
        let draining = draining.clone();
        Arc::new(move || MemberCondition {
            ready: member_ready.load(Ordering::Acquire),
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
    {
        // The shutdown path below keeps the originals of these two; the
        // initialization task works on its own clones. Everything else it
        // needs is captured by the async block directly.
        let reconciliation = reconciliation.clone();
        let draining = draining.clone();
        let redis_url = settings.redis_url.clone();
        let namespace = settings.namespace.clone();
        let registration_config = settings.registration;
        let allow_plaintext = settings.allow_plaintext_transport;
        let cache = settings.binding_cache;
        tokio::spawn(async move {
            loop {
                let connected =
                    RedisRepository::connect(&redis_url, &namespace, Duration::from_secs(3)).await;
                let store = match connected {
                    Ok(store) => Arc::new(store),
                    Err(error) if matches!(error, adx_agent_store::Error::Invalid(_)) => {
                        fatal(format!("Activator store configuration rejected: {error}"));
                    }
                    Err(error) => {
                        retry("Activator store connect", error).await;
                        continue;
                    }
                };
                if let Err(error) = environment.reachable().await {
                    retry("API Server reachability", error).await;
                    continue;
                }
                let activator = match Activator::with_cache(
                    AgentState::new(store.clone()),
                    environment.clone(),
                    cache,
                ) {
                    Ok(activator) => Arc::new(activator),
                    Err(error) => {
                        fatal(format!("Activator cache configuration rejected: {error}"));
                    }
                };
                // The Activator itself is now functional; the member record
                // may report ready from here on. The readiness PROBE stays
                // 503 until registration below has succeeded, so the Gateway
                // never selects an unregistered replica.
                member_ready.store(true, Ordering::Release);
                let registered = match registration_config.clone() {
                    Some(config) => {
                        let registry = match adx_agent_store::discovery::RedisRegistry::new(
                            &redis_url,
                            &namespace,
                            Duration::from_secs(3),
                        ) {
                            Ok(registry) => registry,
                            Err(error) => {
                                fatal(format!("Activator registry rejected: {error}"));
                            }
                        };
                        let started = Reconciliation::start(
                            activator.clone(),
                            store.as_ref().clone(),
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
                let mut slot = reconciliation.lock().unwrap();
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
        });
    }
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
            // Draining comes first: new work is rejected explicitly (callers
            // retry another member) while already-accepted requests keep
            // running. The readiness probe follows the draining flag.
            draining.store(true, Ordering::Release);
            // Abort alias reconciliation, stop lease renewal, then unregister
            // and announce the removal, retried within the shutdown budget;
            // on final failure the lease expiry removes the member.
            let registered = reconciliation.lock().unwrap().take();
            if let Some(reconciliation) = registered {
                if let Err(error) = reconciliation.shutdown(shutdown_timeout).await {
                    tracing::warn!(
                        %error,
                        "Activator reconciliation shutdown failed; lease will expire"
                    );
                }
            }
            // Initialization may have completed (and registered) while the
            // unregister above was retrying: remove any lease that appeared
            // in the meantime too.
            let late = reconciliation.lock().unwrap().take();
            if let Some(reconciliation) = late {
                if let Err(error) = reconciliation.shutdown(shutdown_timeout).await {
                    tracing::warn!(
                        %error,
                        "Activator reconciliation shutdown failed; lease will expire"
                    );
                }
            }
            // Returning stops the accept loop and closes idle connections;
            // in-flight requests finish, each bounded by its request
            // deadline. The watchdog cuts anything still running once the
            // overall exit bound (unregister budget + one request deadline +
            // slack) expires.
            tokio::spawn(async move {
                let bound = shutdown_timeout + timeout + Duration::from_secs(1);
                tokio::time::sleep_until(started + bound).await;
                tracing::warn!(
                    "Activator shutdown budget exhausted; exiting with in-flight work cut"
                );
                std::process::exit(0);
            });
        })
        .await?;
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
    std::process::exit(1);
}
