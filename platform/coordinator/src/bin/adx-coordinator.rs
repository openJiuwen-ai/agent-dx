use adx_coordinator::{
    auth::{AuthRpc, Credential},
    lease::CoordinatorLease,
    readiness,
    rpc::CoordinatorRpc,
    standby::Standby,
    storage::{LegacyCheckpointProofs, RedisStore},
    Placement,
};
use adx_core::Error;
use adx_process::{read_config, shutdown};
use adx_protocol::control as pb;
use adx_transport::tls::TlsFiles;
use serde::Deserialize;
#[cfg(test)]
use std::future::Future;
use std::{net::SocketAddr, path::PathBuf, time::Duration};
use tokio::sync::{oneshot, watch};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Bootstrap {
    key_file: PathBuf,
    #[serde(flatten)]
    credential: Credential,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    #[serde(default)]
    storage_domains: std::collections::BTreeMap<String, adx_core::volumes::StorageDomain>,
    #[serde(default)]
    checkpoint_reclaimers: Vec<adx_checkpoint_storage::reclaim::ReclaimerConfig>,
    #[serde(default)]
    legacy_checkpoint_proofs: LegacyCheckpointProofs,
    listen: SocketAddr,
    #[serde(default)]
    metrics_listen: Option<SocketAddr>,
    #[serde(default)]
    autoscaling_listen: Option<SocketAddr>,
    #[serde(default)]
    readiness_listen: Option<SocketAddr>,
    #[serde(default)]
    advertised_address: Option<String>,
    #[serde(default = "default_heartbeat")]
    heartbeat_timeout_seconds: u64,
    #[serde(default = "default_node_retention")]
    node_retention_seconds: u64,
    #[serde(default = "default_ttl")]
    discovery_ttl_seconds: u64,
    #[serde(default = "default_failed_retention")]
    failed_retention_seconds: u64,
    /// Primary lease budget: how long the epoch-taking right survives
    /// without renewal. The primary renews at lease/3; a renewal that
    /// definitively fails stops the primary immediately. Values are
    /// deployment-tuned (D5); 15s aligns with the discovery TTL.
    #[serde(default = "default_lease_seconds")]
    lease_seconds: u64,
    /// Standby read-only synchronization period. Synchronization is
    /// evidence and readiness only — takeover always re-reads.
    #[serde(default = "default_standby_sync_seconds")]
    standby_sync_seconds: u64,
    /// Handover drain budget: after SIGTERM stops admitting new work,
    /// in-flight work gets this long to finish before the process exits
    /// and releases the lease. Deployment grace periods must cover it.
    #[serde(default = "default_shutdown_timeout_seconds")]
    shutdown_timeout_seconds: u64,
    redis_url: String,
    namespace: String,
    scheduler_shards: usize,
    placement: String,
    rpc_timeout_seconds: u64,
    tls: TlsFiles,
    bootstrap_credentials: Vec<Bootstrap>,
}
fn default_node_retention() -> u64 {
    1800
}
fn default_heartbeat() -> u64 {
    30
}
fn default_ttl() -> u64 {
    15
}
fn default_failed_retention() -> u64 {
    600
}
fn default_lease_seconds() -> u64 {
    15
}
fn default_standby_sync_seconds() -> u64 {
    5
}
fn default_shutdown_timeout_seconds() -> u64 {
    15
}

/// Supervision helper used by the tests below; the production binary runs
/// the primary role through the lease-driven loop in `run_primary`.
#[cfg(test)]
async fn supervise<S, H, B>(
    server: S,
    shutdown_signal: H,
    background: B,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: Future<Output = Result<(), Box<dyn std::error::Error>>>,
    H: Future<Output = ()>,
    B: Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result,
        _ = shutdown_signal => server.await,
        result = background => result,
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]

async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _logging_guard = adx_observability::logging::init("adx-coordinator", false)?;
    let config: Config = read_config()?;
    if config.node_retention_seconds == 0
        || config.heartbeat_timeout_seconds == 0
        || config.discovery_ttl_seconds < 3
        || config.failed_retention_seconds == 0
        || !(1..=600).contains(&config.lease_seconds)
        || config.standby_sync_seconds == 0
        || config.shutdown_timeout_seconds == 0
    {
        return Err(
            "positive node/Failed retention, heartbeat timeout, drain budget, standby sync and lease (1..=600s), and discovery TTL >= 3 required"
                .into(),
        );
    }
    let placement = match config.placement.as_str() {
        "pack" => Placement::Pack,
        "spread" => Placement::Spread,
        _ => return Err("placement must be pack or spread".into()),
    };
    // Validate TLS identities at process start so misconfiguration fails
    // fast in any role; the primary role loads them again when it starts.
    config
        .tls
        .load_rpc(adx_protocol::auth::Principal::Coordinator)?;
    let timeout = Duration::from_secs(config.rpc_timeout_seconds);
    let lease_ttl = Duration::from_secs(config.lease_seconds);
    // Lease operations are bounded well below the lease period, so a
    // single hanging attempt cannot outlive the lease.
    let lease_timeout = timeout.min(lease_ttl / 3);
    if config.rpc_timeout_seconds >= config.lease_seconds {
        adx_observability::warn!(
            "rpc_timeout_seconds ({}s) is not below lease_seconds ({}s); \
             lease operations are bounded by the lease budget instead",
            config.rpc_timeout_seconds,
            config.lease_seconds
        );
    }
    // Readiness serves from process start: probes distinguish "role
    // acquisition or initial synchronization not complete" from a serving
    // replica in any role.
    let readiness = readiness::Readiness::new();
    if let Some(address) = config.readiness_listen {
        let listener = tokio::net::TcpListener::bind(address).await?;
        tokio::spawn(readiness::serve(listener, readiness.clone()));
    }
    // Preparation: bind listeners and connect storage, but never take an
    // epoch — role acquisition comes next. Redis unavailability waits here
    // (with a shutdown escape) rather than crash-looping the process.
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    let store = loop {
        match RedisStore::connect(&config.redis_url, &config.namespace, timeout).await {
            Ok(store) => break store,
            Err(Error::Unavailable(message)) => {
                adx_observability::warn!("Coordinator storage unavailable: {message}");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {},
                    _ = shutdown() => return Ok(()),
                }
            }
            Err(error) => return Err(error.into()),
        }
    };
    let lease = CoordinatorLease::new(&config.redis_url, &config.namespace, lease_timeout)?;
    let holder = uuid::Uuid::new_v4().simple().to_string();
    // Role acquisition: whoever holds the lease may take a new epoch. An
    // inconclusive acquisition (Redis unavailable) may have applied
    // server-side, so it is retried — the next attempt either wins or
    // observes the lease this process already holds. Losing the race
    // means running as the standby — read-only, no begin, no advertise —
    // until the lease is absent or expired and this process wins a
    // takeover attempt.
    loop {
        match lease.acquire(&holder, lease_ttl).await {
            Ok(true) => break,
            Ok(false) => {
                adx_observability::info!("Coordinator lease held elsewhere; entering standby");
                let standby = Standby::new(store.clone());
                match standby
                    .run_until_promotion(
                        &lease,
                        &holder,
                        lease_ttl,
                        Duration::from_secs(config.standby_sync_seconds),
                        &readiness,
                        shutdown(),
                    )
                    .await?
                {
                    true => break,
                    false => return Ok(()),
                }
            }
            Err(Error::Unavailable(message)) => {
                adx_observability::warn!("Coordinator lease acquisition unavailable: {message}");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {},
                    _ = shutdown() => return Ok(()),
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
    adx_observability::info!("Coordinator lease acquired; becoming primary");
    let outcome = run_primary(
        &config,
        config.tls.clone(),
        placement,
        &lease,
        &holder,
        &store,
        listener,
        &readiness,
    )
    .await;
    // Whatever ended the primary role — graceful drain, early failure, or
    // a lost lease — return the lease so a standby does not wait out its
    // expiry. A lost lease makes the release fail harmlessly (someone
    // else holds it now).
    if let Err(error) = lease.release(&holder).await {
        adx_observability::warn!("Coordinator lease release failed; it will expire: {error}");
    }
    outcome
}
/// The primary role: begin with a fresh epoch, serve until shutdown or
/// lease loss, and drain gracefully on shutdown. Returns after the role
/// ends; the caller releases the lease.
#[allow(clippy::too_many_arguments)]
async fn run_primary(
    config: &Config,
    tls: TlsFiles,
    placement: Placement,
    lease: &CoordinatorLease,
    holder: &str,
    store: &RedisStore,
    listener: tokio::net::TcpListener,
    readiness: &readiness::Readiness,
) -> Result<(), Box<dyn std::error::Error>> {
    let (server_tls, client_tls, peers) =
        tls.load_rpc(adx_protocol::auth::Principal::Coordinator)?;
    let timeout = Duration::from_secs(config.rpc_timeout_seconds);
    let lease_ttl = Duration::from_secs(config.lease_seconds);
    // The lease only grants the RIGHT to take an epoch; begin still runs the
    // full storage recovery, retried while the lease is kept alive. Each
    // attempt renews right before begin: a definitive rejection means the
    // takeover right is gone and this process must not advance the epoch.
    // An inconclusive (unavailable) renewal waits and retries instead.
    let session = loop {
        match lease.renew(holder, 0, lease_ttl).await {
            Ok(true) => {}
            Ok(false) => {
                return Err("Coordinator lease lost before begin completed".into());
            }
            Err(error) => {
                adx_observability::warn!(
                    "Coordinator lease renewal unavailable before begin: {error}"
                );
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {},
                    _ = shutdown() => return Ok(()),
                }
                continue;
            }
        }
        match store.begin(config.scheduler_shards).await {
            Ok(session) => break session,
            Err(Error::Unavailable(message)) => {
                adx_observability::warn!("Coordinator storage recovery unavailable: {message}");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {},
                    _ = shutdown() => return Ok(()),
                }
            }
            Err(error) => return Err(error.into()),
        }
    };
    let session = session.with_legacy_checkpoint_proofs(config.legacy_checkpoint_proofs.clone());
    // Bind the epoch we just took to the lease, and gate the whole primary
    // role on it: anything but a successful renewal means the lease was
    // lost during begin (or another instance owns primaryship) and no
    // management task may start. The release happens in main when this
    // function returns, on every path.
    if !matches!(
        lease.renew(holder, session.epoch(), lease_ttl).await,
        Ok(true)
    ) {
        return Err("Coordinator lease lost during begin; not becoming primary".into());
    }
    let mut administrators = Vec::new();
    let mut tenants = Vec::new();
    for credential in &config.bootstrap_credentials {
        let key = std::fs::read_to_string(&credential.key_file)?;
        let entry = (key.trim().to_owned(), credential.credential.clone());
        if entry.1.administrator {
            administrators.push(entry);
        } else {
            tenants.push(entry);
        }
    }
    session.reconcile_administrators(&administrators).await?;
    for (key, credential) in tenants {
        session.bootstrap_credential(&key, &credential).await?;
    }
    let auth = AuthRpc::new(session.clone(), peers.clone());
    let routes = adx_coordinator::routes::RoutePublisher::new(session.clone(), peers.clone());
    let rpc = CoordinatorRpc::with_heartbeat_timeout(
        session.clone(),
        placement,
        peers,
        client_tls,
        timeout,
        Duration::from_secs(config.heartbeat_timeout_seconds),
    )
    .await?;
    let mut reclaimers = adx_checkpoint_storage::reclaim::Reclaimers::default();
    for reclaimer in &config.checkpoint_reclaimers {
        reclaimers.insert(reclaimer.clone().build()?)?;
    }
    let rpc = rpc
        .with_volume_domains(config.storage_domains.clone())?
        .with_reclaimers(reclaimers);
    let metrics_listener = match config.metrics_listen {
        Some(address) => Some(tokio::net::TcpListener::bind(address).await?),
        None => None,
    };
    let metrics_rpc = rpc.clone();
    let metrics_lease = lease.clone();
    let autoscaling_listener = match config.autoscaling_listen {
        Some(address) => Some(tokio::net::TcpListener::bind(address).await?),
        None => None,
    };
    let autoscaling_rpc = rpc.clone();
    let autoscaling = async move {
        if let Some(listener) = autoscaling_listener {
            adx_observability::info!(address=%listener.local_addr()?, "Coordinator autoscaling HTTP listening");
            adx_coordinator::autoscaling::serve(listener, autoscaling_rpc).await?;
        }
        std::future::pending::<std::io::Result<()>>().await
    };
    let metrics = async move {
        if let Some(listener) = metrics_listener {
            adx_coordinator::metrics::serve(listener, metrics_rpc, metrics_lease).await?;
        }
        std::future::pending::<std::io::Result<()>>().await
    };
    routes.refresh().await?;
    let ttl = Duration::from_secs(config.discovery_ttl_seconds);
    if let Some(address) = &config.advertised_address {
        session.advertise(&config.namespace, address, ttl).await?;
    }
    readiness.promote();
    // Background tasks stop between iterations on the stop token: an
    // iteration in flight when the token fires still completes (issued
    // node commands are finished), the next one never starts. Dropping
    // the token sender stops them too, so no writer can outlive this
    // function's scope.
    let (stop_tx, stop_rx) = watch::channel(false);
    let maintenance_tick = Duration::from_secs(
        (config
            .heartbeat_timeout_seconds
            .min(config.discovery_ttl_seconds)
            / 3)
        .max(1),
    );
    let maintenance_ttl = Duration::from_secs(config.discovery_ttl_seconds);
    let maintenance_retention = Duration::from_secs(config.node_retention_seconds);
    let maintenance_advertised = config.advertised_address.clone();
    let maintenance_namespace = config.namespace.clone();
    let maintenance_rpc = rpc.clone();
    let maintenance_session = session.clone();
    let mut maintenance_stop = stop_rx.clone();
    let mut maintenance = tokio::spawn(async move {
        let mut tick = tokio::time::interval(maintenance_tick);
        loop {
            tokio::select! {
                biased;
                changed = maintenance_stop.changed() => {
                    if changed.is_err() || *maintenance_stop.borrow() {
                        return;
                    }
                }
                _ = tick.tick() => {}
            }
            if let Err(error) = maintenance_rpc.expire_nodes().await {
                adx_observability::warn!("node health publication unavailable: {error}");
            }
            if let Err(error) = maintenance_rpc
                .collect_retired_nodes(maintenance_retention)
                .await
            {
                adx_observability::warn!(%error, "offline node retirement deferred");
            }
            if let Some(address) = &maintenance_advertised {
                if let Err(error) = maintenance_session
                    .advertise(&maintenance_namespace, address, maintenance_ttl)
                    .await
                {
                    adx_observability::warn!("Coordinator discovery renewal unavailable: {error}");
                }
            }
        }
    });
    let recovery_rpc = rpc.clone();
    let mut recovery_stop = stop_rx.clone();
    let mut recovery = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(2));
        loop {
            tokio::select! {
                biased;
                changed = recovery_stop.changed() => {
                    if changed.is_err() || *recovery_stop.borrow() {
                        return;
                    }
                }
                _ = tick.tick() => {}
            }
            if let Err(error) = recovery_rpc.recover_environments().await {
                adx_observability::warn!("environment recovery incomplete: {error}");
            }
            if let Err(error) = recovery_rpc.resume_pending().await {
                adx_observability::warn!("pending resume reconciliation unavailable: {error}");
            }
        }
    });
    let preparation_rpc = rpc.clone();
    let mut preparation_stop = stop_rx.clone();
    let mut preparation = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(2));
        loop {
            tokio::select! {
                biased;
                changed = preparation_stop.changed() => {
                    if changed.is_err() || *preparation_stop.borrow() {
                        return;
                    }
                }
                _ = tick.tick() => {}
            }
            if let Err(error) = preparation_rpc.reconcile_snapshot_preparations().await {
                adx_observability::warn!("snapshot preparation reconciliation incomplete: {error}");
            }
        }
    });
    let collection_retention = Duration::from_secs(config.failed_retention_seconds);
    let collection_rpc = rpc.clone();
    let mut collection_stop = stop_rx;
    let mut collection = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        loop {
            tokio::select! {
                biased;
                changed = collection_stop.changed() => {
                    if changed.is_err() || *collection_stop.borrow() {
                        return;
                    }
                }
                _ = tick.tick() => {}
            }
            if let Err(error) = collection_rpc.collect_shared_artifacts().await {
                adx_observability::warn!("shared artifact collection incomplete: {error}");
            }
            if let Err(error) = collection_rpc.collect_snapshots().await {
                adx_observability::warn!("snapshot collection incomplete: {error}");
            }
            if let Err(error) = collection_rpc.expire_dormant().await {
                adx_observability::warn!("dormant expiration incomplete: {error}");
            }
            if let Err(error) = collection_rpc.collect_checkpoint_cleanups().await {
                adx_observability::warn!("checkpoint cleanup incomplete: {error}");
            }
            if let Err(error) = collection_rpc.collect_failed(collection_retention).await {
                adx_observability::warn!("failed environment collection incomplete: {error}");
            }
        }
    });
    // The route publisher is read-only; stopping it mid-publication is
    // safe, so it is aborted rather than stopped between iterations.
    let mut route_task = tokio::spawn(routes.clone().run(Duration::from_millis(200)));
    // Primaryship supervision: keep renewing the lease — through the whole
    // drain — and end the primary role the moment the lease is lost or
    // factually expired, so no write outlives the lease.
    let epoch = session.epoch();
    let mut renewer = tokio::spawn({
        let lease = lease.clone();
        let holder = holder.to_owned();
        async move {
            lease
                .supervise(&holder, epoch, lease_ttl, std::future::pending::<()>())
                .await
        }
    });
    adx_observability::info!("adx-coordinator listening on {}", listener.local_addr()?);
    let mut builder = tonic::transport::Server::builder();
    if let Some(tls) = server_tls {
        builder = builder.tls_config(tls)?;
    }
    // The server stops accepting when the drain sequence reaches its last
    // step; SIGTERM alone does not stop it, because in-flight requests must
    // keep finishing while the lease is still renewed.
    let (server_release, server_shutdown) = oneshot::channel::<()>();
    let mut server = tokio::spawn(
        builder
            .add_service(
                pb::volume_execution_service_server::VolumeExecutionServiceServer::new(rpc.clone()),
            )
            .add_service(pb::volume_service_server::VolumeServiceServer::new(
                rpc.clone(),
            ))
            .add_service(pb::snapshot_service_server::SnapshotServiceServer::new(
                rpc.clone(),
            ))
            .add_service(
                pb::snapshot_preparation_service_server::SnapshotPreparationServiceServer::new(
                    rpc.clone(),
                ),
            )
            .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(rpc.clone()))
            .add_service(pb::credential_service_server::CredentialServiceServer::new(
                auth.clone(),
            ))
            .add_service(pb::auth_service_server::AuthServiceServer::new(auth))
            .add_service(
                pb::route_service_server::RouteServiceServer::new(routes.clone())
                    .max_encoding_message_size(64 * 1024 * 1024),
            )
            .add_service(
                pb::environment_directory_service_server::EnvironmentDirectoryServiceServer::new(
                    routes.clone(),
                )
                .max_encoding_message_size(64 * 1024 * 1024),
            )
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move {
                    let _ = server_shutdown.await;
                },
            ),
    );
    let outcome: Option<Box<dyn std::error::Error>> = tokio::select! {
        _ = shutdown() => {
            drain_primary(
                &rpc,
                &routes,
                readiness,
                &stop_tx,
                [
                    ("maintenance", &mut maintenance),
                    ("recovery", &mut recovery),
                    ("preparation", &mut preparation),
                    ("collection", &mut collection),
                ],
                &mut route_task,
                &mut renewer,
                &mut server,
                server_release,
                Duration::from_secs(config.shutdown_timeout_seconds),
            )
            .await;
            None
        }
        result = &mut renewer => Some(match result {
            Ok(Ok(())) => std::io::Error::other("primary lease supervision ended").into(),
            Ok(Err(error)) => error.into(),
            Err(error) => error.into(),
        }),
        result = &mut maintenance => Some(match result {
            Ok(()) => std::io::Error::other("maintenance task ended").into(),
            Err(error) => error.into(),
        }),
        _result = &mut route_task => Some(std::io::Error::other("route task ended").into()),
        result = &mut collection => Some(match result {
            Ok(()) => std::io::Error::other("collection task ended").into(),
            Err(error) => error.into(),
        }),
        result = &mut recovery => Some(match result {
            Ok(()) => std::io::Error::other("recovery task ended").into(),
            Err(error) => error.into(),
        }),
        result = &mut preparation => Some(match result {
            Ok(()) => std::io::Error::other("preparation task ended").into(),
            Err(error) => error.into(),
        }),
        result = metrics => result.err().map(|error| error.into()),
        result = autoscaling => result.err().map(|error| error.into()),
        result = &mut server => match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error.into()),
            Err(error) => Some(error.into()),
        },
    };
    // On a drain path these already finished (abort is a no-op); on any
    // other exit they must not keep writing after the primary role ends.
    renewer.abort();
    maintenance.abort();
    recovery.abort();
    preparation.abort();
    collection.abort();
    route_task.abort();
    server.abort();
    match outcome {
        Some(error) => Err(error),
        None => Ok(()),
    }
}
/// The graceful handover drain, all within one budget measured from the
/// first step: ① stop admitting new mutating work (structured "not
/// accepted", readiness flips to draining), ② stop the background write
/// tasks between iterations, ③ end the long-lived directory
/// subscriptions (no empty route or environment directory is published;
/// clients reconnect to the next primary), ④ stop accepting new
/// connections and let in-flight requests finish. The lease keeps being
/// renewed throughout; if it is lost mid-drain the drain aborts
/// immediately — a primary must not keep finishing work with a lost
/// lease. The caller releases the lease after this returns.
#[allow(clippy::too_many_arguments)]
async fn drain_primary(
    rpc: &CoordinatorRpc,
    routes: &adx_coordinator::routes::RoutePublisher,
    readiness: &readiness::Readiness,
    stop: &watch::Sender<bool>,
    background: [(&'static str, &mut tokio::task::JoinHandle<()>); 4],
    route_task: &mut tokio::task::JoinHandle<()>,
    renewer: &mut tokio::task::JoinHandle<Result<(), Error>>,
    server: &mut tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    server_release: oneshot::Sender<()>,
    budget: Duration,
) {
    let deadline = tokio::time::Instant::now() + budget;
    let remaining = || deadline.saturating_duration_since(tokio::time::Instant::now());
    // ① Stop admitting new mutating work; the readiness probe reports
    // the drain so the orchestrator can replace this process.
    rpc.begin_drain();
    readiness.drain();
    // ② Stop the background write tasks; an iteration already in flight
    // completes (issued node commands finish), no new one starts.
    stop.send_replace(true);
    for (name, task) in background {
        if tokio::time::timeout(remaining(), &mut *task).await.is_err() {
            task.abort();
            adx_observability::warn!("primary drain budget exhausted before {name} stopped");
        }
    }
    route_task.abort();
    // ③ End long-lived directory subscriptions; nothing empties the
    // published view, so Ingress keeps forwarding on its cached routes.
    routes.close_subscriptions();
    rpc.close_subscriptions();
    // ④ Stop accepting and let in-flight requests finish within the
    // remaining budget. A renewal loss ends the drain immediately
    // instead of finishing "gracefully" against a lost lease.
    let _ = server_release.send(());
    tokio::select! {
        result = &mut *renewer => {
            adx_observability::warn!(
                "primary lease ended during drain; stopping immediately: {:?}",
                result
            );
            server.abort();
        }
        result = &mut *server => match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                adx_observability::warn!("coordinator server ended during drain: {error}")
            }
            Err(error) => {
                adx_observability::warn!("coordinator server task failed during drain: {error}")
            }
        },
        _ = tokio::time::sleep(remaining()) => {
            adx_observability::warn!("primary drain budget exhausted; cutting in-flight work");
            server.abort();
        }
    }
    renewer.abort();
}

#[cfg(test)]
mod tests {
    use super::supervise;
    use std::{
        error::Error,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };
    use tokio::sync::{oneshot, Notify};

    #[tokio::test]
    async fn shutdown_stops_background_polling_while_server_drains() {
        let (signal_tx, signal_rx) = oneshot::channel();
        let (server_shutdown_tx, server_shutdown_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let draining = Arc::new(Notify::new());
        let server_draining = draining.clone();
        let server = async move {
            let _ = server_shutdown_rx.await;
            server_draining.notify_one();
            let _ = release_rx.await;
            Ok::<(), Box<dyn Error>>(())
        };
        let ticks = Arc::new(AtomicUsize::new(0));
        let background_ticks = ticks.clone();
        let mut interval = tokio::time::interval(Duration::from_millis(1));
        let background = std::future::poll_fn(move |cx| {
            while interval.poll_tick(cx).is_ready() {
                background_ticks.fetch_add(1, Ordering::SeqCst);
            }
            std::task::Poll::<Result<(), Box<dyn Error>>>::Pending
        });
        let controller = async {
            signal_tx.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(1), draining.notified())
                .await
                .expect("server did not enter drain");
            let before = ticks.load(Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert_eq!(
                ticks.load(Ordering::SeqCst),
                before,
                "background polled during drain"
            );
            release_tx.send(()).unwrap();
        };
        let (result, ()) = tokio::join!(
            supervise(
                server,
                async move {
                    let _ = signal_rx.await;
                    let _ = server_shutdown_tx.send(());
                },
                background,
            ),
            controller,
        );
        result.unwrap();
    }
}
